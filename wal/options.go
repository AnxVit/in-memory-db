package wal

import "os"

type Options struct {
	NoSync           bool
	SegmentSize      int
	LogFormat        LogFormat
	SegmentCacheSize int
	NoCopy           bool
	DirPerms         os.FileMode
	FilePerms        os.FileMode
	Directory        string

	MaxRingSize int64
}
