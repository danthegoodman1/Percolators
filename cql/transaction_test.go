package cql

import (
	"context"
	"fmt"
	"testing"
)

func TestSetup(t *testing.T) {
	_, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransactionWrite(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv")

	err, _ = func(t Transactable) (error, chan error) {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.Write("examplew", []byte("this is a write only val"))
			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransactionReadWrite(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv")

	err, _ = func(t Transactable) (error, chan error) {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.Write("examplerw", []byte("this is a read write val"))

			// Read cached val
			val, err := tx.Get(ctx, "examplerw")
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got val", string(val))

			// Read networked val
			val, err = tx.Get(ctx, "examplew")
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got networked val", string(val))

			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}
