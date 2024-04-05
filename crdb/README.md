# Percolator for CRDB

## Why CRDB?

CRDB is the most feature-filled and operationally easy database out of the box that I trust with my data. It’s just a good database.

I also have extensive experience running it out to thousands of transactions per second in production workflows, and I understand it’s behavior, scaling patterns, weaknesses, etc. very well.

Additionally, the AS OF SYSTEM TIME condition allows the client to preserve the original snapshot isolation level of Percolator (like serializable, but aborts at commit time).

While we reduce the DB to a KV store, we can use the SQL schema to supplement the metadata rows without too much unraveling.

## CRDB already has transactions, why make this again?

Primarily for learning.

If you want an argument why it might be better than normal transactions, well there are a few benefits that greatly increase the transactional throughput of CRDB:

1. Serializable transactions, using the Read Committed isolation level - This can be achieved because the transaction is managed on the client, not the server
2. Reduced connection pool contention - Because each operation in the transaction is an isolated statement, connections can be used for individual statements only
3. Native retries - CRDB automatically retries statements for various reasons if they are not part of a larger transaction
4. Lower write stalls - Because writes are only performed at commit time, there is an immensely lower chance that operations are stalled because a competing transaction is operating on a row

All this means that you should be able to squeeze more and faster transactions out of CRDB. Yeah it’s reduced to KV, but that’s ok, don’t be afraid.

I’m not arguing this should be used instead of normal SQL transactions, that’s probably a bad idea and this likely has some bugs that I am unaware of.

## Who is the timestamp oracle?

CRDB is, we get a timestamp with a simple `select now()` statement at the beginning of every transaction.

So technically this breaks the isolation guarantees, and a later optimization would be to elect a single CRDB node as the timestamp oracle, or a single process (say a single replica deployment on k8s). I’ll probably extract the timestamp oracle to a function passed into the client initializer if I end up adding proper timestamps, but since I’m making this just for learning at the moment (and I know how to properly implement it), I’m just going to sweep that under the rug and pretend everything is fine.

## Composable transactions

Managing transactions is a pain, and wouldn’t it be nice if you could just write functions?

Composable transactions will automatically start a transaction if one doesn’t exist, or will use an existing one. They will also automatically roll back if an error is thrown (all the way up the chain), or automatically commit.

This means you never need to handle rollbacks or commits, never need to open and close a transaction, never need to manage pool connections, never need to decide between read-only and read-write transactions, and more!

The best database is the one you have to think the least about the rules of when using.

(note: this was inspired by FoundationDB’s client)

However, because a write after the read timestamp would cause a transaction abort at commit time anyway, we could in theory perform the optimization of not using AS OF SYSTEM TIME reads, and just aborting the transaction when we find a row has been updated after the transaction read timestamp.

The reason we don’t do this is because of automatic read-only transactions: A read only transaction never writes, so rather than having to specify read only vs read-write transactions, they become implicitly read-only by just committing without any writes.

While this potentially leaves some optimization on the table, it makes composable transactions very easy to use, and that’s a worthwhile tradeoff.

Could we drop transaction composition and support read-only and read-write transactions to get quicker aborts on conflict? Sure.

Could we have some function-level call that says “hey you can disable read-aborting for this part of the transaction”? Yes, but then you’d have to think a lot more about what kinds of transactions are being used, whether you’re introducing bugs, how the database can be used, etc. That’s a lot of thinking about the database, and not the thing you are trying to make. Bleh.

The transaction would abort regardless, and reads are so much cheaper than writes, so it doesn’t really matter if you end up doing a few extra reads before writing. Plus, you never really have to think about it, and that simplicity is worth a lot.

## Transactional caching

Similar to the awesome composable transactions, the transaction client has another trick up its sleeve: transactional caching.

Lets say you read the same row across two different functions within the same transaction, well it’d sure be a waste to do to the DB multiple times when you know you’re going to get the exact same data! Instead, the client caches reads for a transactions, so when you read the same row again, it instantly returns the previous result without making any network requests.

The same works for writes: If you write to a row, then any subsequent reads will see that write without going to the database.

Keep in mind that this can use a lot of memory.

(note: this was inspired by FoundationDB’s client)

## Snapshot isolation

Simply put, snapshot isolation is just like serializable: It protects against writes against the same key from concurrent transactions, and ensures you see a stable point in time of the database during the entire transaction.

The major difference is that reads never stall (e.g. a row is locked), and writes never fail (e.g. serializable conflict). Transaction abort due to conflicts happens at commit time.

TLDR same thing, only aborts at commit time.

I won’t explain any more features of Percolator (like rolling forward transactions), if you’re interested on how it works and its guarantees, go read the paper (it’s a great read).

## Pro tips

1. Use composable transactions
2. Don’t worry about passing info into transaction functions, just read it again (it’s cached by txn)
3. Don’t use long or massive transactions. The longer they are, the more likely there will be a conflict at commit time (and the consequence of retrying it is higher). Writing a lot of data will also take longer.