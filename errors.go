package db

import "errors"

var (
	ErrorClosedDB = errors.New("DB is closed")

	ErrorsWait = errors.New("wait for flush memtabel")

	ErrTooBig = errors.New("big request size")
)
