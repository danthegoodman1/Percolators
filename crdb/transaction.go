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

	// TODO: prewrite

	// TODO: write

	return nil
}

func (tx *Txn) prewrite(ctx context.Context) error {

}

func (tx *Txn) write(ctx context.Context) error {

}

func (tx *Txn) get(ctx context.Context, key string) (*Record, error) {
	// TODO: If existing txn found, roll it forward if we can
}

func (tx *Txn) Get(ctx context.Context, key string) (any, error) {
	// TODO: Check the read cache
	// TODO: Check the write cache

	record, err := tx.get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error in tx.get: %w", err)
	}
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
