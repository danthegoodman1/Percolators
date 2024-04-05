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

		readCache  map[string]any // TODO: correct type
		writeCache map[string]any // TODO: correct type
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

	// TODO: preWriteAll

	// TODO: writeAll

	return nil
}

func (tx *Txn) preWriteAll(ctx context.Context) error {

}

func (tx *Txn) writeAll(ctx context.Context) error {

}

func (tx *Txn) getRecord(ctx context.Context, key string) (*record, error) {
	// TODO: If existing txn found, roll it forward if we can
}

func (tx *Txn) Get(ctx context.Context, key string) ([]byte, error) {
	// TODO: Check the read cache
	// TODO: Check the write cache

	rec, err := tx.getRecord(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error in tx.get: %w", err)
	}

	return rec.val, nil
}

// TODO: GetRange

// TODO: Write

// TODO: Delete

func (tx *Txn) getTime(ctx context.Context) (*time.Time, error) {
	row := tx.pool.QueryRow(ctx, "select now()")
	var t time.Time
	if err := row.Scan(&t); err != nil {
		return nil, fmt.Errorf("error scanning time: %w", err)
	}

	return &t, nil
}
