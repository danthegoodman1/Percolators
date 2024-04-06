package cql

type (
	record struct {
		Key string
		Col string
		Ts  int64
		Val []byte
	}
)
