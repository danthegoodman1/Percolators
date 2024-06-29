package cql

import "context"

type (
	ReadFuture struct {
		readChan        chan readError
		immediateReturn []byte
	}

	readError struct {
		Record *record
		Err    error
	}
)

// Waits on the read
func (rf ReadFuture) Get(ctx context.Context) ([]byte, error) {
	// If we don't have a channel, then use the bytes
	if rf.readChan == nil {
		return rf.immediateReturn, nil
	}

	// Otherwise wait for the channel
	select {
	case r := <-rf.readChan:
		if r.Err != nil {
			return nil, r.Err
		}
		var val []byte
		if r.Record != nil {
			val = r.Record.Val
		}
		return val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
