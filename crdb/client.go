package crdb

import (
	"context"
	"fmt"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	Client struct {
		pool  *pgxpool.Pool
		table string
	}
)

func NewClient(pool *pgxpool.Pool, table string) *Client {
	c := &Client{
		pool:  pool,
		table: table,
	}

	return c
}

func (c *Client) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	tx := &Txn{
		pool:  c.pool,
		table: c.table,
		id:    uuid.New().String(),
		pendingWrites: btree.NewG(2, func(a, b pendingWrite) bool {
			return a.Key < b.Key
		}),
	}

	// Get the read timestamp
	t, err := tx.getTime(ctx)
	if err != nil {
		return fmt.Errorf("error getting read timestamp: %w", err)
	}
	tx.readTime = t

	err = fn(ctx, tx)
	if err != nil {
		return fmt.Errorf("error executing transaction: %w", err)
	}

	if tx.pendingWrites.Len() == 0 {
		// Read only transaction
		return nil
	}

	err = tx.commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
