package sstable

type Options struct {
	CompactionInterval int
	BlockSize          int64
	CacheSize          int
}
