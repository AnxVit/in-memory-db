package db

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
	SkipListOpt  skiplist.Options
	MemtableSize int64
	NumImmutable int64
	MemtableDir  string

	// SSTable options
	SSTableOpt          sstable.Options
	TableSize           int64
	TableSizeMultiplier int64

	// Level Controller options
	NumCompactors       int64
	LevelSizeMultiplier int64
	BaseLevelSize       int64
	NumLevelZeroTables  int64
	MaxLevels           int

	// Manifest
	ManifestDir string

	Directory      string
	LastCompaction time.Time
	Compress       bool

	maxRequestSize int64
}
