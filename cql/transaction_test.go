package cql

import (
	"context"
	"testing"
)

func TestSetup(t *testing.T) {
	_, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransaction(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv")

	err, _ = func(t Transactable) (error, chan error) {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.Write("example", []byte("this is a val"))
			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}
