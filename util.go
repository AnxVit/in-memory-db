package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"
	"time"
)

type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() interface{}
	Valid() bool

	Close() error
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func serializeOp(key, value []byte) []byte {
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	enc := gob.NewEncoder(buf)

	operation := Entry{
		Key:   key,
		Value: value,
	}

	err := enc.Encode(&operation)
	if err != nil {
		return nil
	}

	return buf.Bytes()
}

func desialaizeOp(data []byte) (Entry, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()
	buf.Write(data)

	dec := gob.NewDecoder(buf)

	var operation Entry
	err := dec.Decode(&operation)
	if err != nil {
		return Entry{}, err
	}
	return operation, nil
}

func KeyWithTs(key []byte) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], uint64(time.Now().UnixMicro()))
	return out
}

func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(key)-8:])
}
