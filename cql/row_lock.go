package cql

import (
	"encoding/json"
	"fmt"
)

type rowLock struct {
	PrimaryLockKey string `json:"pri"`
	StartTs        int64  `json:"sts"`
	TimeoutTs      int64  `json:"tts"`
}

func (l *rowLock) Encode() (string, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func parseLock(s string) (*rowLock, error) {
	var l rowLock
	err := json.Unmarshal([]byte(s), &l)
	if err != nil {
		return nil, fmt.Errorf("error in json.Unmarshal: %w", err)
	}

	return &l, nil
}
