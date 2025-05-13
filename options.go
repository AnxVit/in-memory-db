package db

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/sstable"
	"GoKeyValueWarehouse/wal"
)

type Options struct {
	// WAL options
	WALOpt wal.Options

	// SkipList options
	SkipListOpt skiplist.Options

	// Memtable options
	MemtableSize int64
	NumImmutable int64

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

	Directory string

	MaxRequestSize uint64
}
