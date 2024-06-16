package cql

import (
	"context"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/table"
	"golang.org/x/sync/errgroup"
	"time"
)

type (
	Txn struct {
		session *gocql.Session
		table   string
		id      string

		readTime  *time.Time
		writeTime *time.Time

		readCache map[string][]byte

		// empty array means delete
		pendingWrites  map[string][]byte
		primaryLockKey string

		timeout time.Time

		tableMetadata     table.Metadata
		isolationLevel    IsolationLevel
		serialConsistency gocql.SerialConsistency
		onLock            LockBehavior
	}

	IsolationLevel string
)

var (
	// ReadRepeatable means that if there is a commit to a record between read and write, the txn does not abort
	ReadRepeatable IsolationLevel = "rr"
	// Snapshot isolation is effectively the same as Serializable
	Snapshot IsolationLevel = "ss"
)

func (tx *Txn) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	return fn(ctx, tx)
}

// SetIsolationLevel sets the highest possible isolation level for the transaction.
// If it has already been explicitly set at something higher, it cannot be reduced (and will be a no-op).
// Default Snapshot
func (tx *Txn) SetIsolationLevel(isolationLevel IsolationLevel) {
	if tx.isolationLevel == Snapshot && isolationLevel == ReadRepeatable {
		// Cannot be reduced
		return
	}
	tx.isolationLevel = isolationLevel
}

// SetSerialConsistency sets the SERIAL CONSISTENCY of the batch operations.
// Default gocql.Serial
func (tx *Txn) SetSerialConsistency(consistency gocql.SerialConsistency) {
	tx.serialConsistency = consistency
}

// commit is the top level commit called by the transaction initializer.
// It coordinates prewrite and write. It returns a synchronous error if the transaction
// cannot complete, and an async error channel for the async completion of the transaction.
func (tx *Txn) commit(ctx context.Context) error {
	ts, err := tx.getTime(ctx)
	if err != nil {
		return fmt.Errorf("error getting write timestamp: %w", err)
	}

	tx.writeTime = ts

	err = tx.preWriteAll(ctx)
	if err != nil {
		return fmt.Errorf("error in transaction pre write: %w", err)
	}

	err = tx.writeAll(ctx)
	if err != nil {
		return fmt.Errorf("error in transaction write: %w", err)
	}

	return nil
}

func (tx *Txn) preWriteAll(ctx context.Context) error {
	for key, val := range tx.pendingWrites {
		// Determine the primary rowLock key
		tx.primaryLockKey = key

		err := tx.preWrite(ctx, key, val)
		if err != nil {
			return fmt.Errorf("error in primary lock prewrite: %w", err)
		}

		break
	}

	// Can do prewrite concurrently
	g := errgroup.Group{}
	for key, val := range tx.pendingWrites {
		if key == tx.primaryLockKey {
			// Ignore this one
			continue
		}
		g.Go(func() error {
			return tx.preWrite(ctx, key, val)
		})
	}

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("error in async prewrite: %w", err)
	}

	return nil
}

func (tx *Txn) preWrite(ctx context.Context, key string, val []byte) error {
	b := tx.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	b.SerialConsistency(tx.serialConsistency)
	// Write rowLock and data to primary key
	lock := rowLock{
		PrimaryLockKey: tx.primaryLockKey,
		StartTs:        tx.readTime.UnixNano(),
		TimeoutTs:      tx.timeout.UnixNano(),
		CommitTs:       tx.writeTime.UnixNano(),
	}

	encodedLock, err := lock.Encode()
	if err != nil {
		return fmt.Errorf("error in rowLock.Encode: %w", err)
	}

	// Insert the lock
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, 0, 'l', ?) if not exists", tx.table),
		Args: []any{key, []byte(encodedLock)},
	})

	// Insert the data record
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'd', ?) if not exists", tx.table),
		Args: []any{key, tx.readTime.UnixNano(), val},
	})

	// Non-map version was always having applied: false
	applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("error in MapExecuteBatchCAS: %w", err)
	}

	if !applied {
		return fmt.Errorf("%w: prewrite not applied (confict)", &TxnAborted{})
	}

	if tx.isolationLevel == Snapshot || tx.isolationLevel == "" {
		// Snapshot isolation, verify that there are no write records after our TS
		// Get the highest write record
		err = tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts > ? and col = 'w' order by ts asc limit 1", tx.table), key, tx.readTime.UnixNano()).Consistency(gocql.Quorum).Scan()
		if errors.Is(err, gocql.ErrNotFound) {
			// There is nothing higher
			return nil
		} else if err != nil {
			return fmt.Errorf("error scanning snapshot write commit row: %w", err)
		}

		// Otherwise we found a committed write, and need to abort
		return fmt.Errorf("%w: Snapshot isolation failure: found committed write higher than read timestamp for key %s", &TxnAborted{}, key)
	}

	return nil
}

func (tx *Txn) writeAll(ctx context.Context) error {
	// Primary rowLock commit
	{
		b := tx.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		b.SerialConsistency(tx.serialConsistency)
		// Remove the lock (encode is deterministic)
		lock := rowLock{
			PrimaryLockKey: tx.primaryLockKey,
			StartTs:        tx.readTime.UnixNano(),
			TimeoutTs:      tx.timeout.UnixNano(),
			CommitTs:       tx.writeTime.UnixNano(),
		}
		encodedLock, err := lock.Encode()
		if err != nil {
			return fmt.Errorf("error in lock.Encode: %w", err)
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
			Args: []any{tx.primaryLockKey, []byte(encodedLock)},
		})

		// Insert the write record
		wr := writeRecord{
			StartTimeNS: tx.readTime.UnixNano(),
		}
		encodedWrite, err := wr.Encode()
		if err != nil {
			return fmt.Errorf("error in writeRecord.Encode: %w", err)
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'w', ?) if not exists", tx.table),
			Args: []any{tx.primaryLockKey, tx.writeTime.UnixNano(), []byte(encodedWrite)},
		})

		applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
		if err != nil {
			return fmt.Errorf("error in primary lock MapExecuteBatchCAS: %w", err)
		}

		if !applied {
			return fmt.Errorf("%w: primary lock write not applied (confict)", &TxnAborted{})
		}
	}

	// Update the rest of the keys with write record async (any future reads will roll forward)
	go func(ctx context.Context) {
		b := tx.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		b.SerialConsistency(tx.serialConsistency)
		for key := range tx.pendingWrites {
			if key == tx.primaryLockKey {
				// Ignore this one, we already handled it
				continue
			}

			// Remove the lock (encode is deterministic)
			lock := rowLock{
				PrimaryLockKey: tx.primaryLockKey,
				StartTs:        tx.readTime.UnixNano(),
				TimeoutTs:      0, // not needed since not primary lock
				CommitTs:       tx.writeTime.UnixNano(),
			}
			encodedLock, err := lock.Encode()
			if err != nil {
				// TODO fmt.Errorf("error in lock.Encode: %w", err)
				return
			}

			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
				Args: []any{tx.primaryLockKey, []byte(encodedLock)},
			})

			// Insert the write record
			wr := writeRecord{
				StartTimeNS: tx.readTime.UnixNano(),
			}
			encodedWrite, err := wr.Encode()
			if err != nil {
				// TODO: fmt.Errorf("error in writeRecord.Encode: %w", err)
				return
			}

			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'w', ?) if not exists", tx.table),
				Args: []any{tx.primaryLockKey, tx.writeTime.UnixNano(), []byte(encodedWrite)},
			})
		}
	}(ctx)

	return nil
}

// getRecord will get a record from the DB.
func (tx *Txn) getRecord(ctx context.Context, key string, ts time.Time) (*record, error) {
	lockRec := record{
		Key: key,
	}
	foundLock := true
	err := tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts = 0 and col = 'l'", tx.table), key).Consistency(gocql.Quorum).Scan(&lockRec.Col, &lockRec.Ts, &lockRec.Val)
	if errors.Is(err, gocql.ErrNotFound) {
		foundLock = false
	} else if err != nil {
		return nil, fmt.Errorf("error scanning lock row: %w", err)
	}

	dataRec := record{
		Key: key,
	}
	err = tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and col = 'd' and ts < ? order by ts desc limit 1", tx.table), key, tx.readTime.UnixNano()).Consistency(gocql.Quorum).Scan(&dataRec.Col, &dataRec.Ts, &dataRec.Val)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error scanning data row: %w", err)
	}

	// Check if we found a row lock
	if foundLock {
		lock, err := parseLock(string(lockRec.Val))
		if err != nil {
			return nil, fmt.Errorf("error in parseLock: %w", err)
		}

		var primaryLock *rowLock

		if lock.PrimaryLockKey != key {
			// We need to look up the primary and see if the locks exists (we might be primary)
			primaryLockRec := record{
				Key: key,
			}
			err = tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts = 0 and col = 'l'", tx.table), lock.PrimaryLockKey).Consistency(gocql.Quorum).Scan(&primaryLockRec.Col, &primaryLockRec.Ts, &primaryLockRec.Val)
			if errors.Is(err, gocql.ErrNotFound) {
				// Do nothing
			} else if err != nil {
				return nil, fmt.Errorf("error scanning primary lock row: %w", err)
			} else {
				// We've got the primary lock
				primaryLock, err = parseLock(string(primaryLockRec.Val))
			}
		} else {
			// We were already the primary lock
			primaryLock = lock
		}

		if primaryLock != nil && primaryLock.TimeoutTs <= time.Now().UnixNano() {
			// The transaction has expired, roll it back
			err = tx.rollBackTxn(ctx, key, lock.StartTs, lockRec.Val)
			if err != nil {
				return nil, fmt.Errorf("error in tx.rollBackTxn: %w", err)
			}

			// Do lookup again
			return tx.getRecord(ctx, key, ts)
		}

		if primaryLock == nil {
			// We must check for the write record
			foundCommitWrite := false
			iter := tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts > ? and col = 'w' order by ts asc", tx.table), lock.PrimaryLockKey, lock.StartTs).Consistency(gocql.Quorum).Iter()
			scanner := iter.Scanner()

			for scanner.Next() {
				rec := record{
					Key: key,
				}
				err = scanner.Scan(&rec.Col, &rec.Ts, &rec.Val)
				if err != nil {
					return nil, fmt.Errorf("error scanning primary commit write row: %w", err)
				}

				// Check if the write record contains the start timestamp
				wr, err := parseWriteRecord(string(rec.Val))
				if err != nil {
					return nil, fmt.Errorf("error in parseWriteRecord: %w", err)
				}

				if wr.StartTimeNS == lock.StartTs {
					// We found the write record, we can abort
					foundCommitWrite = true

					// only need to manually close the iterator if we break early
					err = iter.Close()
					if err != nil {
						return nil, fmt.Errorf("error in iter.Close: %w", err)
					}

					break
				}
			}

			if foundCommitWrite {
				// Txn is committed, roll the transaction forward
				err = tx.rollForward(ctx, key, *lock)
				if err != nil {
					return nil, fmt.Errorf("error in tx.rollForward for %s: %w", key, err)
				}

				// Do lookup again
				return tx.getRecord(ctx, key, ts)
			}

			// Otherwise the primary lock was rolled back, so we need to as well
			err = tx.rollBackTxn(ctx, key, lock.StartTs, lockRec.Val)
			if err != nil {
				return nil, fmt.Errorf("error in tx.rollBackTxn: %w", err)
			}

			// Do lookup again
			return tx.getRecord(ctx, key, ts)
		}

		// Otherwise the txn is in progress
		if tx.onLock == OnLockReadPrevious {
			// Try to read a previous value (1ns before)
			return tx.getRecord(ctx, key, time.Unix(0, lock.StartTs).Add(-time.Nanosecond))
		}

		// Abort
		return nil, fmt.Errorf("%w: existing lock found on %s (another txn in progress)", &TxnAborted{}, key)
	}

	// Return the data row
	if dataRec.Val == nil {
		// Empty
		return nil, nil
	}
	return &dataRec, nil
}

// getRange will get a range of records from the DB. If no atTime is provided, then it will abort.
func (tx *Txn) getRange(ctx context.Context, key string, atTime *time.Time) (*record, error) {
	// TODO: Get records and locks, check if we need to roll back or forward
	panic("not implemented")
}

// rollForward will attempt to roll a transaction forward if possible. Otherwise,
// it will abort the transaction
func (tx *Txn) rollForward(ctx context.Context, key string, lock rowLock) error {
	b := tx.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	b.SerialConsistency(tx.serialConsistency)
	// Remove the lock (encode is deterministic)
	encodedLock, err := lock.Encode()
	if err != nil {
		return fmt.Errorf("error in lock.Encode: %w", err)
	}

	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
		Args: []any{key, []byte(encodedLock)},
	})

	// Insert the write record
	wr := writeRecord{
		StartTimeNS: lock.StartTs,
	}
	encodedWrite, err := wr.Encode()
	if err != nil {
		return fmt.Errorf("error in writeRecord.Encode: %w", err)
	}

	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'w', ?) if not exists", tx.table),
		Args: []any{key, lock.CommitTs, []byte(encodedWrite)},
	})

	applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("error in roll forward lock MapExecuteBatchCAS: %w", err)
	}

	if !applied {
		return fmt.Errorf("%w: roll forward not applied (confict)", &TxnAborted{})
	}

	return nil
}

func (tx *Txn) rollBackTxn(ctx context.Context, key string, ts int64, lockRec []byte) error {
	// If the txn is expired, roll it back
	b := tx.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	b.SerialConsistency(tx.serialConsistency)
	// Delete the lock
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
		Args: []any{key, lockRec},
	})
	// Delete the data record
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = ? and col = 'd' if exists", tx.table),
		Args: []any{key, ts},
	})
	applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("error in executing LWT to roll back expired lock on %s: %w", key, err)
	}
	if !applied {
		return fmt.Errorf("%w: found lock roll back not applied on %s", &TxnAborted{}, key)
	}

	return nil
}

func (tx *Txn) Get(ctx context.Context, key string) ([]byte, error) {
	// Check the pending writes first to get a more updated value
	if val, exists := tx.pendingWrites[key]; exists {
		return val, nil
	}

	// Check the read cache
	if val, exists := tx.readCache[key]; exists {
		return val, nil
	}

	rec, err := tx.getRecord(ctx, key, *tx.readTime)
	if err != nil {
		return nil, fmt.Errorf("error in tx.get: %w", err)
	}

	if rec == nil {
		return nil, nil
	}

	return rec.Val, nil
}

// TODO: GetRange (maybe reads locks first, then reads read cache, then reads data records?)

func (tx *Txn) Write(key string, value []byte) {
	// Store in write cache
	tx.pendingWrites[key] = value

	// Delete from read cache if we have it (since we will read from write queue anyway)
	delete(tx.readCache, key)
}

func (tx *Txn) Delete(key string) {
	// Just writing empty
	tx.Write(key, []byte{})
}

func (tx *Txn) getTime(ctx context.Context) (*time.Time, error) {
	t := time.Now()
	return &t, nil
}

type TxnAborted struct {
}

func (t *TxnAborted) Error() string {
	return "transaction aborted"
}
