package main

import (
	"GoKeyValueWarehouse/operation"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
)

func serializeOp(op operation.OPR_CODE, key, value interface{}) []byte {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)

	operation := operation.Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	err := enc.Encode(&operation)
	if err != nil {
		return nil
	}

	return buf.Bytes()
}
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// ParseTs parses the timestamp from the key bytes.
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}
