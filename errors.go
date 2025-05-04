package main

import "errors"

var (
	ErrorClosedDB = errors.New("DB is closed")

	ErrorsWait = errors.New("wait for flush memtabel")
)
