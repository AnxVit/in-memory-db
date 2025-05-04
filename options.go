package main

import (
	"GoKeyValueWarehouse/levels/sstable"
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/wal"
	"time"
)

type Options struct {
	// WAL options
	WALOpt wal.Options

	// memtable options
	MemtableOpt  skiplist.Options
	MemtableSize int64

	// sstable options
	SSTableOpt          sstable.Options
	TableSize           int64
	TableSizeMultiplier int64

	NumCompactors       int64
	LevelSizeMultiplier int64
	BaseLevelSize       int64
	NumLevelZeroTables  int64

	MaxLevels int

	Directory      string
	LastCompaction time.Time
	Compress       bool
}
