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

func (tx *Txn) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	return fn(ctx, tx)
}

// commit is the top level commit called by the transaction initializer.
// It coordinates prewrite and write
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
		// Determine the primary lock key
		tx.primaryLockKey = key
		break
	}

	b := tx.session.NewBatch(gocql.UnloggedBatch)

	for key, val := range tx.pendingWrites {
		// Write lock and data to primary key
		lock := lock{
			PrimaryLockKey: tx.primaryLockKey,
			StartTs:        tx.readTime.UnixNano(),
		}

		encodedLock, err := lock.Encode()
		if err != nil {
			return fmt.Errorf("error in lock.Encode: %w", err)
		}

		// Insert the lock
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, 0, 'l', ?) if not exists", tx.table),
			Args: []any{key, encodedLock},
		})

		// Insert the data record
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: fmt.Sprintf("insert into \"%s\" (key, ts, col, val) values (?, ?, 'd', ?) if not exists", tx.table),
			Args: []any{key, tx.readTime.UnixNano(), val},
		})
	}

	applied, _, err := tx.session.ExecuteBatchCAS(b)
	if err != nil {
		return fmt.Errorf("error in ExecuteBatchCAS: %w", err)
	}

	if !applied {
		return fmt.Errorf("%w: prewrite not applied (confict)", &TxnAborted{})
	}

	return nil
}

func (tx *Txn) writeAll(ctx context.Context) error {
	// TODO: Verify that we have the primary key still, commit it
	// TODO: - write the commit intent
	// TODO: - remove the lock

	// TODO: Start the commit by updating the primary key (remove from map so we don't double apply)

	// TODO: Update the rest of the keys with write record async (any future reads will roll forward)
	for key, val := range tx.pendingWrites {
		if key == tx.primaryLockKey {
			// Ignore this one, we already handled it
			continue
		}
	}
}

// getRecord will get a record from the DB. If no atTime is provided, then it will use the current time.
func (tx *Txn) getRecord(ctx context.Context, key string, ts time.Time) (*record, error) {
	b := tx.session.NewBatch(gocql.UnloggedBatch) // can do unlogged since we're only hitting 1 partition (this is the default)
	// Select the data before the timestamp
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and col = 'd' order by ts desc limit 1", tx.table),
		Args: []any{key},
	})
	// Select the lock
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: fmt.Sprintf("select col, ts, val from \"%s\" where key = ? and ts = 0 and col = 'l'"),
		Args: []any{key},
	})

	var m map[string]any
	applied, iter, err := tx.session.MapExecuteBatchCAS(b, m)
	if err != nil {
		return nil, fmt.Errorf("error in MapExecuteBatchCAS: %w", err)
	}
	if !applied {
		return nil, fmt.Errorf("%w: not applied", &TxnAborted{})
	}

	var rows []record
	scanner := iter.Scanner()
	for scanner.Next() {
		rec := record{
			Key: key,
		}
		err = scanner.Scan(&rec.Col, &rec.Ts, &rec.Val)
		if err != nil {
			return nil, fmt.Errorf("%w: error scanning row: %w", &TxnAborted{}, err)
		}
	}

	// Check if either has a lock
	var foundLock *lock
	for _, rec := range rows {
		if rec.Col == "l" {
			foundLock, err = parseLock(string(rec.Val))
			if err != nil {
				return nil, fmt.Errorf("%w: error in parseLock: %w", &TxnAborted{}, err)
			}
			break
		}
	}

	if foundLock != nil {
		if foundLock.PrimaryLockKey == key {
			// We are the primary
			if foundLock.TimeoutTs < time.Now().UnixNano() {
				// TODO: roll it back
				// Do lookup again
				return tx.getRecord(ctx, key, ts)
			}
			// TODO: wait or immediately abort?
		}
		// Otherwise it's a secondary lock
		// TODO: Get the primary lock
		// TODO: Check if we need to roll forward, and roll forward
	}

	// Return the data row
	for _, rec := range rows {
		if rec.Col == "d" {
			return &rec, nil
		}
	}

	// Not found
	return nil, nil
}

// getRange will get a range of records from the DB. If no atTime is provided, then it will abort.
func (tx *Txn) getRange(ctx context.Context, key string, atTime *time.Time) (*record, error) {
	// TODO: If existing txn found, roll it forward if we can
}

// rollForward will attempt to roll a transaction forward if possible. Otherwise,
// it will abort the transaction
func (tx *Txn) rollForward(ctx context.Context, key string) (*record, error) {

}

func (tx *Txn) rollbackOrphanedTxn(ctx context.Context) error {
	// TODO: walk the linked list in each direction, wait on the primary lock until all other records are undone
}

func (tx *Txn) Get(ctx context.Context, key string) ([]byte, error) {
	// Check the read cache
	if val, exists := tx.readCache[key]; exists {
		return val, nil
	}

	// Check the pending writes
	if val, exists := tx.pendingWrites[key]; exists {
		return val, nil
	}

	rec, err := tx.getRecord(ctx, key, tx.readTime)
	if err != nil {
		return nil, fmt.Errorf("error in tx.get: %w", err)
	}

	return rec.val, nil
}

// TODO: GetRange (does not use read cache)c

func (tx *Txn) Write(key string, value []byte) {
	// TODO: if record already exists, replace it (probably need a tree)

	// Store in write cache
	tx.pendingWrites[key] = value

	// Store in read cache
	tx.readCache[key] = value
}

func (tx *Txn) Delete(key string) {
	// Just writing empty
	tx.Write(key, []byte{})
}

func (tx *Txn) getTime(ctx context.Context) (*time.Time, error) {
	row := tx.session.QueryRow(ctx, "select now()")
	var t time.Time
	if err := row.Scan(&t); err != nil {
		return nil, fmt.Errorf("error scanning time: %w", err)
	}

	return &t, nil
}

type TxnAborted struct {
}

func (t *TxnAborted) Error() string {
	return "transaction aborted"
}
