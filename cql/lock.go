package cql

import (
	"encoding/json"
	"fmt"
)

type lock struct {
	PrimaryLockKey string `json:"pri"`
	StartTs        int64  `json:"sts"`
	TimeoutTs      int64  `json:"tts"`
}

func (l *lock) Encode() (string, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func parseLock(s string) (*lock, error) {
	var l lock
	err := json.Unmarshal([]byte(s), &l)
	if err != nil {
		return nil, fmt.Errorf("error in json.Unmarshal: %w", err)
	}

	return &l, nil
}
