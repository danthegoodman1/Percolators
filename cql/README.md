# CQL Percolator

## Why CQL?

CQL does not natively have proper transactions (if you say LWTs I swear to god), but it has atomic batches, which are required to implement Percolator.

Cassandra and Scylla (Scylla more so) allow absurd levels of scale, and in particular, write performance.

Adding serializable transactions on top is a tantalizing experience.

## Who is the timestamp oracle?

Currently, the client is.

So technically this breaks the isolation guarantees, but I’m just going to sweep that under the rug and pretend everything is fine for now (very easy to add a timestamp oracle anyway).

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

Let's say you read the same row across two different functions within the same transaction, well it’d sure be a waste to do to the DB multiple times when you know you’re going to get the exact same data! Instead, the client caches reads for a transactions, so when you read the same row again, it instantly returns the previous result without making any network requests.

The same works for writes: If you write to a row, then any subsequent reads will see that write without going to the database.

Keep in mind that this can use a lot of memory.

(note: this was inspired by FoundationDB’s client)

Note: `GetRange` does not use read caching, this will fetch from the DB every time.

## Snapshot isolation

Simply put, snapshot isolation is just like serializable: It protects against writes against the same key from concurrent transactions, and ensures you see a stable point in time of the database during the entire transaction.

The major difference is that reads never stall (e.g. a row is locked), and writes never fail (e.g. serializable conflict). Transaction abort due to conflicts happens at commit time.

TLDR same thing, only aborts at commit time.

I won’t explain any more features of Percolator (like rolling forward transactions), if you’re interested on how it works and its guarantees, go read the paper (it’s a great read).

## Transaction records

This process uses the same format as TiKV's implementation

```
CF_DEFAULT:  (key, 'd', start_ts)   -> value
CF_LOCK:     (key, 'l', 0)          -> lock_info [primary key, start_ts, timeout_ts]
CF_WRITE:    (key, 'w', commit_ts)  -> write_info [start_ts, op]
```

Keys are encoded by `(key, column, timestamp)` in the table schema.

Because the SQL lacks traditional atomic batch operations, we need to break up write into 2 stages.

For prewrite, we can dual-insert both the data, and the lock like a normal atomic batch.

For write, we need to both delete the lock, and insert the write record in the same atomic transaction (assuming that we still have the lock). To use a transaction for this purpose would be comical.

## Rollbacks

We handle rollbacks implicitly. That is to say, we don't roll back: we instead treat catastrophic and graceful transaction aborts as the same (by just hard aborting), and let Percolator handle on-demand rollbacks of different KV pairs.

## Cleaning old keys

During a get operation, if there are multiple committed rows older than the grace period (default 12 hours), then they will be deleted async in the background. This ensures that lots of writes don't pile up and cause a single key to store a massive amount of data.

## Pro tips

1. Use composable transactions
2. Don’t worry about passing info into transaction functions, just read it again (it’s cached by txn)
3. Don’t use long or massive transactions. The longer they are, the more likely there will be a conflict at commit time (and the consequence of retrying it is higher). Writing a lot of data will also take longer.