package cql

import "context"

type (
	Transactable interface {
		Transact(ctx context.Context, fn func(ctx context.Context, tx *Txn) error) error
	}
)
