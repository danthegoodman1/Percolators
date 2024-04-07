package cql

import (
	"context"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/table"
	"strconv"
	"strings"
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

		tableMetadata table.Metadata
	}
)

func (tx *Txn) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) (error, chan error) {
	return fn(ctx, tx), nil // can return nil, since only used at top level
}

// commit is the top level commit called by the transaction initializer.
// It coordinates prewrite and write. It returns a synchronous error if the transaction
// cannot complete, and an async error channel for the async completion of the transaction.
func (tx *Txn) commit(ctx context.Context) (error, chan error) {
	ts, err := tx.getTime(ctx)
	if err != nil {
		return fmt.Errorf("error getting write timestamp: %w", err), nil
	}

	tx.writeTime = ts

	err = tx.preWriteAll(ctx)
	if err != nil {
		return fmt.Errorf("error in transaction pre write: %w", err), nil
	}

	err, errChan := tx.writeAll(ctx)
	if err != nil {
		return fmt.Errorf("error in transaction write: %w", err), errChan
	}

	return nil, errChan
}

var ErrInvalidKey = errors.New("invalid key")

func parseLockKey(key string) (id string, lockTime time.Time, primaryKey string, err error) {
	parts := strings.Split(key, "::")
	if len(parts) != 3 {
		err = ErrInvalidKey
		return
	}

	id = parts[0]

	parsed, err := strconv.Atoi(parts[1])
	if err != nil {
		return
	}

	lockTime = time.Unix(0, int64(parsed))
	primaryKey = parts[3]

	return
}

func (tx *Txn) preWriteAll(ctx context.Context) error {
	for key, _ := range tx.pendingWrites {
		// Determine the primary rowLock key
		tx.primaryLockKey = key
		break
	}

	b := tx.session.NewBatch(gocql.UnloggedBatch)

	for key, val := range tx.pendingWrites {
		// Write rowLock and data to primary key
		lock := rowLock{
			PrimaryLockKey: tx.primaryLockKey,
			StartTs:        tx.readTime.UnixNano(),
			TimeoutTs:      tx.readTime.Add(time.Second * 5).UnixNano(),
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
	}

	// Non-map version was always having applied: false
	applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("error in MapExecuteBatchCAS: %w", err)
	}

	if !applied {
		return fmt.Errorf("%w: prewrite not applied (confict)", &TxnAborted{})
	}

	return nil
}

func (tx *Txn) writeAll(ctx context.Context) (error, chan error) {
	// Primary rowLock commit
	{
		b := tx.session.NewBatch(gocql.UnloggedBatch)
		// Remove the lock (encode is deterministic)
		lock := rowLock{
			PrimaryLockKey: tx.primaryLockKey,
			StartTs:        tx.readTime.UnixNano(),
			TimeoutTs:      tx.readTime.Add(time.Second * 5).UnixNano(),
		} // TODO: timeout ts
		encodedLock, err := lock.Encode()
		if err != nil {
			return fmt.Errorf("error in lock.Encode: %w", err), nil
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
			Args: []any{tx.primaryLockKey, []byte(encodedLock)},
		})
		fmt.Println(encodedLock)

		// Insert the write record
		wr := writeRecord{
			DataTimeNS: tx.readTime.UnixNano(),
		}
		encodedWrite, err := wr.Encode()
		if err != nil {
			return fmt.Errorf("error in writeRecord.Encode: %w", err), nil
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'w', ?) if not exists", tx.table),
			Args: []any{tx.primaryLockKey, tx.writeTime.UnixNano(), []byte(encodedWrite)},
		})

		applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
		if err != nil {
			return fmt.Errorf("error in primary lock MapExecuteBatchCAS: %w", err), nil
		}

		if !applied {
			return fmt.Errorf("%w: primary lock write not applied (confict)", &TxnAborted{}), nil
		}
	}

	asyncErrChan := make(chan error, 1)
	// Update the rest of the keys with write record async (any future reads will roll forward)
	go func(asyncErrChan chan error) {
		b := tx.session.NewBatch(gocql.UnloggedBatch)
		for key, _ := range tx.pendingWrites {
			if key == tx.primaryLockKey {
				// Ignore this one, we already handled it
				continue
			}

			// Remove the lock (encode is deterministic)
			lock := rowLock{
				PrimaryLockKey: tx.primaryLockKey,
				StartTs:        tx.readTime.UnixNano(),
			} // TODO: timeout ts
			encodedLock, err := lock.Encode()
			if err != nil {
				asyncErrChan <- fmt.Errorf("error in lock.Encode: %w", err)
				return
			}

			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
				Args: []any{tx.primaryLockKey, []byte(encodedLock)},
			})

			// Insert the write record
			wr := writeRecord{
				DataTimeNS: tx.readTime.UnixNano(),
			}
			encodedWrite, err := wr.Encode()
			if err != nil {
				asyncErrChan <- fmt.Errorf("error in writeRecord.Encode: %w", err)
				return
			}

			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'w', ?) if not exists", tx.table),
				Args: []any{tx.primaryLockKey, tx.writeTime.UnixNano(), []byte(encodedWrite)},
			})
		}
	}(asyncErrChan)

	return nil, asyncErrChan
}

// getRecord will get a record from the DB. If no atTime is provided, then it will use the current time.
func (tx *Txn) getRecord(ctx context.Context, key string, ts time.Time) (*record, error) {
	lockRec := record{
		Key: key,
	}
	foundLock := true
	err := tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts = 0 and col = 'l'", tx.table), key).Scan(&lockRec.Col, &lockRec.Ts, &lockRec.Val)
	if errors.Is(err, gocql.ErrNotFound) {
		foundLock = false
	} else if err != nil {
		return nil, fmt.Errorf("error scanning lock row: %w", err)
	}

	dataRec := record{
		Key: key,
	}
	err = tx.session.Query(fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and col = 'd' order by ts desc limit 1", tx.table), key).Scan(&dataRec.Col, &dataRec.Ts, &dataRec.Val)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error scanning data row: %w", err)
	}

	// Check if we found a row lock
	if foundLock {
		fmt.Println("found an existing lock!")
		lock, err := parseLock(string(lockRec.Val))
		if err != nil {
			return nil, fmt.Errorf("error in parseLock: %w", err)
		}
		if lock.PrimaryLockKey == key {
			// We are the primary
			if lock.TimeoutTs <= time.Now().UnixNano() {
				// Roll it back
				b := tx.session.NewBatch(gocql.UnloggedBatch)
				b.Entries = append(b.Entries, gocql.BatchEntry{
					Stmt: fmt.Sprintf("delete from \"%s\" where key = ? and ts = 0 and col = 'l' if val = ?", tx.table),
					Args: []any{tx.primaryLockKey, lockRec.Val},
				})
				applied, _, err := tx.session.MapExecuteBatchCAS(b, make(map[string]interface{}))
				if err != nil {
					return nil, fmt.Errorf("error in executing LWT to roll back expired lock on %s: %w", key, err)
				}
				if !applied {
					return nil, fmt.Errorf("%w: found lock roll back not applied on %s", &TxnAborted{}, key)
				}

				// Do lookup again
				return tx.getRecord(ctx, key, ts)
			}
			return nil, fmt.Errorf("%w: existing lock found on %s", &TxnAborted{}, key)
		}
		// Otherwise it's a secondary rowLock
		// TODO: Get the primary rowLock
		// TODO: Check if we need to roll forward, and roll forward
	}

	// Return the data row
	return &dataRec, nil
}

// getRange will get a range of records from the DB. If no atTime is provided, then it will abort.
func (tx *Txn) getRange(ctx context.Context, key string, atTime *time.Time) (*record, error) {
	// TODO: If existing txn found, roll it forward if we can
	panic("not implemented")
}

// rollForward will attempt to roll a transaction forward if possible. Otherwise,
// it will abort the transaction
func (tx *Txn) rollForward(ctx context.Context, key string) (*record, error) {
	// TODO: this
	panic("not implemented")
}

func (tx *Txn) rollbackOrphanedTxn(ctx context.Context) error {
	// TODO: walk the linked list in each direction, wait on the primary rowLock until all other records are undone
	panic("not implemented")
}

func (tx *Txn) Get(ctx context.Context, key string) ([]byte, error) {
	// Check the read cache
	if val, exists := tx.readCache[key]; exists {
		fmt.Println("got in read cache")
		return val, nil
	}

	// Check the pending writes
	if val, exists := tx.pendingWrites[key]; exists {
		fmt.Println("got in write cache")
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

// TODO: GetRange (does not use read cache)

func (tx *Txn) Write(key string, value []byte) {
	// Store in write cache
	tx.pendingWrites[key] = value
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
