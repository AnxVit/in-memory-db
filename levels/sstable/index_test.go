package sstable

import (
	"bytes"
	"log"
	"testing"
)

func TestIndex(t *testing.T) {
	entries := []Entry{
		{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		},
		{
			Key:   []byte("key2"),
			Value: []byte("value2"),
		},
	}

	table, _ := CreateTable(1, entries, Options{})
	defer func() {
		table.Close()
	}()

	index, err := table.RebuildBlockIndexFromFile(1000)
	if err != nil {
		log.Fatalf("could not get index: %v", err)
		t.Error(err)
	}

	if len(index) == 0 {
		t.Error("could not get index")
	}
	if !bytes.Equal(index[0].key, []byte("key1")) {
		t.Error("wrong key")
	}
}
