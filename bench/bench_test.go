package redis_benchmark

import (
	db "GoKeyValueWarehouse"
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/sstable"
	"GoKeyValueWarehouse/wal"
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

var (
	ctx         = context.Background()
	redisClient *redis.Client
	dbClient    *db.DB
)

func init() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password
		DB:       0,  // Default DB
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}

	opt := db.Options{
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
		MaxRequestSize: 10000000,
	}

	dbClient, err = db.Open(opt)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to DB: %v", err))
	}
}

func BenchmarkRedis_Set(b *testing.B) {
	key := make([]byte, 16)
	value := make([]byte, 100)
	rand.Read(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key-%s-%d", key, i)
		err := redisClient.Set(ctx, k, value, 0).Err()
		if err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}

func BenchmarkRedis_Get(b *testing.B) {
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		err := redisClient.Set(ctx, k, fmt.Sprintf("value-%d", i), 0).Err()
		if err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%10000]
		_, err := redisClient.Get(ctx, k).Result()
		if err != nil && err != redis.Nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkDB_Set(b *testing.B) {
	key := make([]byte, 16)
	rand.Read(key)
	defer dbClient.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key-%s-%d", key, i)
		entry := &db.Entry{
			Key:   []byte(k),
			Value: []byte{},
		}
		entries := append([]*db.Entry{}, entry)
		err := dbClient.Write(entries)
		if err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}

func BenchmarkDB_Get(b *testing.B) {
	keys := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		entry := &db.Entry{
			Key:   []byte(k),
			Value: []byte{},
		}
		entries := append([]*db.Entry{}, entry)
		err := dbClient.Write(entries)
		if err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%10000]
		_, err := dbClient.Get([]byte(k))
		if err != nil && err != redis.Nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}
