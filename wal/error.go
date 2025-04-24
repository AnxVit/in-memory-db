package wal

import "errors"

var (
	ErrCorrupt = errors.New("log corrupt")

	ErrClosed = errors.New("log closed")

	ErrNotFound = errors.New("not found")

	ErrOutOfOrder = errors.New("out of order")

	ErrOutOfRange = errors.New("out of range")
)
