package crdb

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	Client struct {
		pool *pgxpool.Pool
	}
)

func (c *Client) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	tx := &Txn{
		pool: c.pool,
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

	err = tx.commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
}
