package main

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/sstable"
	"GoKeyValueWarehouse/wal"
	"time"
)

type Options struct {
	// WAL options
	WALOpt wal.Options

	// memtable options
	MemtableOpt skiplist.Options

	// sstable options
	SSTableOpt sstable.Options

	Directory      string
	LastCompaction time.Time
	Compress       bool
}
