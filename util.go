package main

import (
	"GoKeyValueWarehouse/operation"
	"bytes"
	"encoding/gob"
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
