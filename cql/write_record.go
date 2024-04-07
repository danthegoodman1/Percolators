package cql

import (
	"encoding/json"
	"fmt"
)

type writeRecord struct {
	StartTimeNS int64 `json:"ts"`
}

func (l *writeRecord) Encode() (string, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func parseWriteRecord(s string) (*writeRecord, error) {
	var l writeRecord
	err := json.Unmarshal([]byte(s), &l)
	if err != nil {
		return nil, fmt.Errorf("error in json.Unmarshal: %w", err)
	}

	return &l, nil
}
