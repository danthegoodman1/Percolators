package crdb

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type (
	Txn struct {
		pool *pgxpool.Pool

		readTime  *time.Time
		writeTime *time.Time

		readCache map[string][]byte

		// empty array means delete
		pendingWrites  map[string][]byte
		primaryLockKey string
	}
)

func (tx *Txn) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	return fn(ctx, tx)
}

// commit is the top level commit called by the transaction initializer.
// It coordinates prewrite and write
func (tx *Txn) commit(ctx context.Context) error {
	wt, err := tx.getTime(ctx)
	if err != nil {
		return fmt.Errorf("error getting write time: %w", err)
	}

	tx.writeTime = wt

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
	// Pick a primary lock key
	for key, _ := range tx.pendingWrites {
		// Just get the first one randomly
		tx.primaryLockKey = key
		break
	}

	// TODO: Write lock to key

	for key, val := range tx.pendingWrites {
		if key == tx.primaryLockKey {
			// Ignore this one, we already handled it
			continue
		}
	}
}

func (tx *Txn) writeAll(ctx context.Context) error {
	// TODO: Verify that we have the primary key still

	// TODO: Start the commit by updating the primary key (remove from map so we don't double apply)

	// TODO: Update the rest of the keys
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
}

// getRange will get a range of records from the DB. If no atTime is provided, then it will abort.
func (tx *Txn) getRange(ctx context.Context, key string, atTime *time.Time) (*record, error) {
	// TODO: If existing txn found, roll it forward if we can
}

// rollForward will attempt to roll a transaction forward if possible. Otherwise,
// it will abort the transaction
func (tx *Txn) rollForward(ctx context.Context, key string) (*record, error) {

}

func (tx *Txn) Get(ctx context.Context, key string) ([]byte, error) {
	// Check the read cache
	if val, exists := tx.readCache[key]; exists {
		return val, nil
	}
	// Check the write cache
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

func (tx *Txn) Write(key string, value []byte) error {
	// Store in write cache
	tx.pendingWrites[key] = value

	return nil
}

func (tx *Txn) Delete(key string) error {
	// Store in write cache
	tx.pendingWrites[key] = []byte{}

	return nil
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
