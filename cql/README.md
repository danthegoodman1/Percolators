# CQL Percolator

<!-- TOC -->
* [CQL Percolator](#cql-percolator)
  * [Why CQL?](#why-cql)
  * [Who is the timestamp oracle?](#who-is-the-timestamp-oracle)
  * [Composable transactions](#composable-transactions)
  * [Serializable Transactions/Distributed Transaction Processing (advanced)](#serializable-transactionsdistributed-transaction-processing-advanced)
  * [Transactional caching](#transactional-caching)
  * [Snapshot isolation](#snapshot-isolation)
  * [Read Repeatable isolation](#read-repeatable-isolation)
  * [Transaction records](#transaction-records)
  * [Rollbacks](#rollbacks)
  * [Async commit](#async-commit)
  * [Timeouts](#timeouts)
  * [Pro tips](#pro-tips)
<!-- TOC -->

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

The reason we don’t do this is because of automatic read-only transactions: A read only transaction never writes, so rather than having to specify read only vs read-write transactions, they become implicitly read-only by just committing without any writes.

While this potentially leaves some optimization on the table, it makes composable transactions very easy to use, and that’s a worthwhile tradeoff.

Could we drop transaction composition and support read-only and read-write transactions to get quicker aborts on conflict? Sure.

Could we have some function-level call that says “hey you can disable read-aborting for this part of the transaction”? Yes, but then you’d have to think a lot more about what kinds of transactions are being used, whether you’re introducing bugs, how the database can be used, etc. That’s a lot of thinking about the database, and not the thing you are trying to make. Bleh.

The transaction would abort regardless, and reads are so much cheaper than writes, so it doesn’t really matter if you end up doing a few extra reads before writing. Plus, you never really have to think about it, and that simplicity is worth a lot.

## Serializable Transactions/Distributed Transaction Processing (advanced)

Transactions can be serialized and passed to other services across the network. This allows for distributed transaction processing across clients.

You can serialize (and abort committing) any running transaction. The REQUIRED steps are:

1. Serialize the transaction, not performing any more operations on the transaction
2. Send the serialized transaction to another service
3. Return `TxnSerialized{}` to the transaction handler function (this prevents the client from committing)

```go
// Your transaction handler function 
t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
    tx.Write("examplew", []byte("this is a write only val"))
    // Serialize the state, you MUST cease future operations within this
    // transaction if you use the serialized transaction elsewhere
    txnBytes, err := tx.Serialize()
    if err != nil {
        return fmt.Errorf("error in tx.Serialize: %w", err)
    }
	
    // ... send to another service
    
    return TxnSerialized{} // Tell the client that it should NOT commit this transaction
})
```

They can be used later like:
```go
txn, err := FromSerialized(s, serialized)
if err != nil {
    t.Fatal(err)
}

err = yourFunction(txn, ...)
if err != nil {
    t.Fatal(err)
}
// The transaction has been committed
```

See an example in `transaction_test.go`.

**It is critical to perform this properly, otherwise your transactions will never commit.**

Note that transactions are not hard-disabled once serialized, as _technically_ you can never deserialize the transaction and continue using the local version, but in fairness there's no real case where this should be needed either.

Deserialized transactions will inherit a deadline in this order:

1. The provided context if it has a deadline
2. The deserialized deadline from the original transaction (will have been 5 seconds in the future)

Transactions can be continuously serialized and deserialized to pass a single transaction among many services, provided that they do not reach their deadline before doing so.

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

## Read Repeatable isolation

Percolator can be tweaked to allow a sort of "read repeatable" (currently still aborts if it finds a running transaction on a row) where instead of checking that there are no commits after our read time during prewrite, we just lock regardless.

While we still have a stable snapshot in time of the database, we do not abort if there was a write to any record between the read timestamp and the commit timestamp.

This can be set with `Transaction.SetIsolationLevel()`, default `Snapshot`. Once explicitly declared as `Snapshot`, it cannot be further reduced to `ReadRepeatable`.

This means that commits are slightly faster, because there is one less round of selecting a record for every write intent.

## Transaction records

This process uses the same format as TiKV's implementation

```
CF_DEFAULT:  (key, 'd', start_ts)   -> value
CF_LOCK:     (key, 'l', 0)          -> lock_info [primary key, start_ts, timeout_ts, commit_ts]
CF_WRITE:    (key, 'w', commit_ts)  -> write_info [start_ts, op]
```

Keys are encoded by `(key, column, timestamp)` in the table schema.

Because the SQL lacks traditional atomic batch operations, we need to break up write into 2 stages.

For prewrite, we can dual-insert both the data, and the lock like a normal atomic batch.

For write, we need to both delete the lock, and insert the write record in the same atomic transaction (assuming that we still have the lock). To use a transaction for this purpose would be comical.

## Rollbacks

We handle rollbacks implicitly. That is to say, we don't roll back: we instead treat catastrophic and graceful transaction aborts as the same (by just hard aborting), and let Percolator handle on-demand rollbacks of different KV pairs.

## Async commit

Once the primary lock is fully committed, the top-level `Transact` call exits, and passes a `chan error` that can be listened for errors in secondary commits.

This is acceptable because of the 2-phase commit, we know that if the rest of the commit fails, another transaction will be able to roll forward any partial commits found on secondary writes.

## Timeouts

By default, transactions have a 5-second timeout (the child context will be given a timeout). If a context with a deadline is a provided, then the timeout will use that instead. BE CAREFUL when using timeouts longer than 5 seconds, as this increases the chance of contention and stalls (e.g. a transaction dies, and another cannot clean up because the timeout has not passed).

## Pro tips

1. Use composable transactions
2. Don’t worry about passing info into transaction functions, just read it again (it’s cached by txn)
3. Don’t use long or massive transactions. The longer they are, the more likely there will be a conflict at commit time (and the consequence of retrying it is higher). Writing a lot of data will also take longer.
4. Retry transactions, and let their backoff window be longer than the transaction timeout (in case you discover a freshly abandoned transaction). Generally leaving the transaction timeout at 5s is a smart idea. You can easily rip the old deadline off a context while preserving its value by using `context.WithoutCancel()`, and letting the transaction client automatically add the 5-second deadline back in