package cql

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type (
	Client struct {
		session *gocql.Session
		table   string
	}
)

func NewClient(session *gocql.Session, table string) *Client {
	c := &Client{
		session: session,
		table:   table,
	}

	return c
}

func (c *Client) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	tx := &Txn{
		session:       c.session,
		table:         c.table,
		id:            uuid.New().String(),
		pendingWrites: make(map[string][]byte),
		readCache:     make(map[string][]byte),
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

	if len(tx.pendingWrites) == 0 {
		// Read only transaction
		return nil
	}

	err = tx.commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
