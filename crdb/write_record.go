package crdb

type writeRecord struct {
	// p (put), d (del)
	Op          string `json:"op"`
	WriteTimeNS int64  `json:"ts"`
}
