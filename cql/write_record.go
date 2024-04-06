package cql

import "encoding/json"

type writeRecord struct {
	DataTimeNS int64 `json:"ts"`
}

func (l *writeRecord) Encode() (string, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
