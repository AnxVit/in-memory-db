package db

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/sstable"
	"GoKeyValueWarehouse/wal"
	"fmt"
	"math/rand"
	"testing"
)

func Test_Main(b *testing.T) {
	opt := Options{
		WALOpt: wal.Options{
			NoSync:           true,
			SegmentSize:      10000,
			LogFormat:        wal.Binary,
			SegmentCacheSize: 10000,
			NoCopy:           true,
			DirPerms:         0755,
			FilePerms:        0755,
			Directory:        "warehouse/mem",
		},
		SkipListOpt: skiplist.Options{
			DeleteExpired:  false,
			MaxLevel:       12,
			P:              0.25,
			FlushThreshold: 10000,
		},

		MemtableSize: 10000,
		NumImmutable: 2,

		SSTableOpt: sstable.Options{
			BlockSize: 10000,
			CacheSize: 10000,
		},
		TableSize:           10000,
		TableSizeMultiplier: 10,

		NumCompactors:       2,
		LevelSizeMultiplier: 10,
		BaseLevelSize:       100000,
		NumLevelZeroTables:  10,
		MaxLevels:           3,

		ManifestDir: "warehouse/manifest",

		Directory:      "warehouse",
		MaxRequestSize: 100000,
	}

	dbClient, err := Open(opt)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to DB: %v", err))
	}
	key := make([]byte, 16)
	rand.Read(key)

	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%s-%d", key, i)
		entry := &Entry{
			Key:   []byte(k),
			Value: []byte{},
		}
		entries := append([]*Entry{}, entry)
		err := dbClient.Write(entries)
		if err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}
