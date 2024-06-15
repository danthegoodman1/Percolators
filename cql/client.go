package cql

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"time"
)

type (
	Client struct {
		session *gocql.Session
		table   string
		onLock  LockBehavior
	}

	// LockBehavior is the behavior when a live lock is discovered during a transaction.
	// Default OnLockAbort
	LockBehavior string
)

var (
	// OnLockAbort aborts the transaction when a live lock is found
	OnLockAbort LockBehavior = "Abort"
	// OnLockReadPrevious reads a previous committed value (if exists) when a live lock is found.
	// The transaction will still abort at commit time if you attempt to write to the key
	// and a later write has committed, or the lock is still active.
	// This is useful for read-only queries, as they will never abort.
	OnLockReadPrevious LockBehavior = "ReadPrevious"
)

func NewClient(session *gocql.Session, table string, onLock LockBehavior) *Client {
	c := &Client{
		session: session,
		table:   table,
		onLock:  onLock,
	}

	return c
}

func (c *Client) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	timeout := time.Now().Add(time.Second * 5)
	if deadline, ok := ctx.Deadline(); ok {
		timeout = deadline
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
	}
	tx := &Txn{
		session:           c.session,
		table:             c.table,
		id:                uuid.New().String(),
		pendingWrites:     make(map[string][]byte),
		readCache:         make(map[string][]byte),
		isolationLevel:    Snapshot,
		onLock:            c.onLock,
		timeout:           timeout,
		serialConsistency: gocql.Serial,
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
