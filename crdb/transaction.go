package crdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"strconv"
	"strings"
	"time"
)

type (
	Txn struct {
		pool  *pgxpool.Pool
		table string
		id    string

		readTime  *time.Time
		writeTime *time.Time

		readCache map[string][]byte

		// empty array means delete
		pendingWrites map[string][]byte
	}

	pendingWrite struct {
		Key string
		Val []byte
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

func (tx *Txn) getPrimaryLockKey() string {
	var key string
	tx.pendingWrites.Ascend(func(item pendingWrite) bool {
		item.Key = key
		return false
	})

	return key
}

func (tx *Txn) preWriteAll(ctx context.Context) (err error) {
	// This is so disgustingly inefficient, I want my mommy
	// TODO: map then array? tree then array? need to be able to ref forward and back one in deterministic order
	primaryKey := tx.getPrimaryLockKey()
	prevKey := ""
	tx.pendingWrites.Ascend(func(item pendingWrite) bool {
		// Write lock and data to primary key
		lock := Lock{
			PrimaryLockKey: primaryKey,
			TxnID:          tx.id,
		}
		if prevKey != "" {
			lock.PreviousKey = prevKey
			tx.pendingWrites.Get()
		}

		encodedLock, err := lock.Encode()
		if err != nil {
			err = fmt.Errorf("error in lock.Encode: %w", err)
			return false
		}

		res, err := tx.pool.Exec(ctx, fmt.Sprintf("upsert into %s (key, val, lock) values ($1, $2, $3) where lock is null and last_write < $4", tx.table), pw.Key, pw.Val, encodedLock, tx.readTime)
		if err != nil {
			err = fmt.Errorf("error writing lock for key %s: %w", pw.Key, err)
			return false
		}

		if res.RowsAffected() == 0 {
			err = fmt.Errorf("failed to write lock for key %s: %w", pw.Key, TxnAborted{})
			return false
		}

		prevKey = item.Key
		return true
	})
	if err != nil {
		return
	}
	a := tx.pendingWrites.Iter()
}

func (tx *Txn) writeAll(ctx context.Context) error {
	// TODO: Verify that we have the primary key still, commit it

	// TODO: Start the commit by updating the primary key (remove from map so we don't double apply)

	// TODO: Update the rest of the keys with write record
	for key, val := range tx.pendingWrites {
		if key == tx.primaryLockKey {
			// Ignore this one, we already handled it
			continue
		}
	}
}

// getRecord will get a record from the DB. If no atTime is provided, then it will use the current time.
func (tx *Txn) getRecord(ctx context.Context, key string, atTime *time.Time) (*record, error) {
	// TODO: If existing txn found, roll it forward if we can
	// TODO: if some data records are more than grace period, delete them
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
	row := tx.pool.QueryRow(ctx, "select now()")
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
