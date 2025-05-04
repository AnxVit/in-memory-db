package main

var TOMBSTONE = []byte("TOMBSTONE")

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64 // time.Unix
	version   uint64
}

type request struct {
	Entries []*Entry
}
