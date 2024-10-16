package cql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"time"
)

type (
	serializableTxn struct {
		Table string
		Id    string

		ReadTime  *time.Time
		WriteTime *time.Time

		PendingWrites  map[string][]byte
		PrimaryLockKey string

		Timeout time.Time

		IsolationLevel    IsolationLevel
		SerialConsistency gocql.Consistency
		OnLock            LockBehavior
	}

	// DeserializedTransaction can be used once
	DeserializedTransaction struct {
		txn *Txn
	}
)

func (s serializableTxn) toTxn(session *gocql.Session) Txn {
	return Txn{
		session:           session,
		table:             s.Table,
		id:                s.Id,
		readTime:          s.ReadTime,
		writeTime:         s.WriteTime,
		readCache:         make(map[string][]byte),
		pendingWrites:     s.PendingWrites,
		primaryLockKey:    s.PrimaryLockKey,
		timeout:           s.Timeout,
		isolationLevel:    s.IsolationLevel,
		serialConsistency: s.SerialConsistency,
		onLock:            s.OnLock,
	}
}

func FromSerialized(session *gocql.Session, serialized []byte) (Transactable, error) {
	d := DeserializedTransaction{}
	var stxn serializableTxn
	err := json.Unmarshal(serialized, &stxn)
	if err != nil {
		return nil, fmt.Errorf("error in json.Unmarshal: %w", err)
	}

	localTxn := stxn.toTxn(session)
	d.txn = &localTxn

	return d, nil
}

func (d DeserializedTransaction) Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error {
	if _, ok := ctx.Deadline(); !ok {
		// If we don't have a context with a deadline to override the timeout, use the txn timeout
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d.txn.timeout)
		defer cancel()
	}

	// Get the read timestamp
	t, err := d.txn.getTime(ctx)
	if err != nil {
		return fmt.Errorf("error getting read timestamp: %w", err)
	}
	d.txn.readTime = t

	err = fn(ctx, d.txn)
	if err != nil {
		return fmt.Errorf("error executing transaction: %w", err)
	}

	if len(d.txn.pendingWrites) == 0 {
		// Read only transaction
		return nil
	}

	err = d.txn.commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
