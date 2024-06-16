package cql

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"math"
	"testing"
)

func TestSetup(t *testing.T) {
	_, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransactionWriteDefault(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.Write("examplew", []byte("this is a write only val"))
			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransactionWriteLocalSerial(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.SetSerialConsistency(gocql.LocalSerial)
			tx.Write("examplew", []byte("this is a write only val"))
			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleTransactionReadWriteSnapshot(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	err = func(t Transactable) error {
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

func TestSingleTransactionReadWriteReadRepeatable(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			tx.Write("examplerwrr", []byte("this is a read write val"))

			tx.SetIsolationLevel(ReadRepeatable)

			// Read cached val
			val, err := tx.Get(ctx, "examplerwrr")
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

func TestRollForward(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	key := "rollfwd"
	pkey := "pkeyrollfwd"
	wr := writeRecord{
		StartTimeNS: 1,
	}
	wrEncoded, err := wr.Encode()
	if err != nil {
		t.Fatal(err)
	}

	lockRec := rowLock{
		PrimaryLockKey: pkey,
		StartTs:        1,
		CommitTs:       2,
		TimeoutTs:      math.MaxInt64,
	}
	encodedLock, err := lockRec.Encode()
	if err != nil {
		t.Fatal(err)
	}

	// The secondary write
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", key, lockRec.StartTs, []byte("rolled fwd")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'l', ?)", key, 0, encodedLock).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// The primary lock
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", pkey, lockRec.StartTs, []byte("primary rolled fwd")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'w', ?)", pkey, lockRec.CommitTs, []byte(wrEncoded)).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// Do the txn that reads it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got rolled forward val:", string(val))

			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestRollBackPrimaryExpired(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	key := "rollbackexpr"
	pkey := "pkeyrollbackexpr"

	lockRec := rowLock{
		PrimaryLockKey: pkey,
		StartTs:        1,
		CommitTs:       2,
		TimeoutTs:      0,
	}
	encodedLock, err := lockRec.Encode()
	if err != nil {
		t.Fatal(err)
	}

	// The secondary write
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", key, 0, []byte("rolled back")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", key, lockRec.StartTs, []byte("rolled fwd")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'l', ?)", key, 0, encodedLock).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// The primary lock
	primaryLockRec := rowLock{
		PrimaryLockKey: pkey,
		StartTs:        1,
		CommitTs:       2,
		TimeoutTs:      0,
	}
	primaryEncodedLock, err := primaryLockRec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'l', ?)", pkey, 0, primaryEncodedLock).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// Do the txn that reads it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got rolled back val:", string(val))

			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestRollBackPrimaryExpiredIsPrimary(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	pkey := "pkeyrollbackexpr-iampk"

	lockRec := rowLock{
		PrimaryLockKey: pkey,
		StartTs:        1,
		CommitTs:       2,
		TimeoutTs:      0,
	}
	encodedLock, err := lockRec.Encode()
	if err != nil {
		t.Fatal(err)
	}

	// The secondary write
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", pkey, 0, []byte("rolled back")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", pkey, lockRec.StartTs, []byte("rolled fwd")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'l', ?)", pkey, 0, encodedLock).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// Do the txn that reads it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, pkey)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got rolled back val:", string(val))

			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestRollBackNoPrimary(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	key := "rollbackexpr"
	pkey := "pkeyrollbackexpr"

	lockRec := rowLock{
		PrimaryLockKey: pkey,
		StartTs:        1,
		CommitTs:       2,
		TimeoutTs:      0,
	}
	encodedLock, err := lockRec.Encode()
	if err != nil {
		t.Fatal(err)
	}

	// The secondary write
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", key, 0, []byte("rolled back")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'd', ?)", key, lockRec.StartTs, []byte("rolled fwd")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = c.session.Query("insert into testkv (key, ts, col, val) values (?, ?, 'l', ?)", key, 0, encodedLock).Exec()
	if err != nil {
		t.Fatal(err)
	}

	// Do the txn that reads it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			fmt.Println("got rolled back val:", string(val))

			return nil
		})
	}(c)

	if err != nil {
		t.Fatal(err)
	}
}

func TestRollBackAbort(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	key := "rollbackexpr-abort"

	// Do the txn that reads it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			if val != nil {
				return fmt.Errorf("got a value? expected nil")
			}

			tx.Write(key, []byte("blah"))

			val, err = tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}

			fmt.Println("got wrote val:", string(val))

			return nil
		})
	}(c)
	if err != nil {
		t.Fatal(err)
	}

	// Abort a transaction that edits it
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			_, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}

			// Write something
			tx.Write(key, []byte("i will not show I'm aborted"))

			fmt.Println("aborting txn")

			return fmt.Errorf("I am some error")
		})
	}(c)
	if err == nil {
		t.Fatal("did not abort")
	}

	// Verify it's still the correct value
	err = func(t Transactable) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read networked val
			val, err := tx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			if string(val) != "blah" {
				return fmt.Errorf("got unexpected value")
			}

			return nil
		})
	}(c)
	if err != nil {
		t.Fatal(err)
	}
}

func TestComposableTransactions(t *testing.T) {
	s, err := setupTest()
	if err != nil {
		t.Fatal(err)
	}

	doAnotherThing := func(t Transactable, someReadKey string) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Read the key (cached)
			val, err := tx.Get(ctx, someReadKey)
			if err != nil {
				return fmt.Errorf("error in tx.Get: %w", err)
			}
			if string(val) != "i'm a val" {
				return fmt.Errorf("got unexpected value: %s", string(val))
			}

			return nil
		})
	}

	doSomething := func(t Transactable, someWriteKey string) error {
		return t.Transact(context.Background(), func(ctx context.Context, tx *Txn) error {
			// Write the key
			tx.Write(someWriteKey, []byte("i'm a val"))

			return doAnotherThing(tx, someWriteKey)
		})
	}

	c := NewClient(s, "testkv", OnLockReadPrevious)

	err = doSomething(c, "composable key")
	if err != nil {
		t.Fatal(err)
	}
}
