package db

import (
	"bytes"
	"testing"
)

func TestSerialize_DesializeOp(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	data := serializeOp(key, value)

	entry, err := desialaizeOp(data)
	if err != nil {
		t.Fatal(err)
	}
	if !(bytes.Equal(entry.Key, key) && bytes.Equal(entry.Value, value)) {
		t.Log(string(entry.Key), string(key))
		t.Log(string(entry.Value), string(value))
		t.Fatal("not equal")
	}

}
