package crdb

import (
	"encoding/json"
)

type Lock struct {
	PrimaryLockKey string `json:"pri"`
	PreviousKey    string `json:"prv"`
	NextKey        string `json:"nxt,omitempty"`
	TxnID          string `json:"id"`
}

func (l *Lock) Encode() (string, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
