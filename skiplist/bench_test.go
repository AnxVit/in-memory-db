package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkSkipList_Insert(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	key := make([]byte, 16)
	value := make([]byte, 100)
	rand.Read(key)
	rand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := append(key, []byte(fmt.Sprintf("%d", i))...)
		sl.Insert(k, value, nil)
	}
}

func BenchmarkSkipList_InsertWithTTL(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	key := make([]byte, 16)
	value := make([]byte, 100)
	ttl := time.Second * 10
	rand.Read(key)
	rand.Read(value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := append(key, []byte(fmt.Sprintf("%d", i))...)
		sl.Insert(k, value, &ttl)
	}
}

func BenchmarkSkipList_Search(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		sl.Insert(key, value, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%10000]
		sl.Search(key)
	}
}

func BenchmarkSkipList_Delete(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		sl.Insert(key, value, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%10000]
		sl.Delete(key)
	}
}

func BenchmarkSkipList_ConcurrentInsert(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 16)
		value := make([]byte, 100)
		rand.Read(key)
		rand.Read(value)
		i := 0
		for pb.Next() {
			k := append(key, []byte(fmt.Sprintf("%d", i))...)
			sl.Insert(k, value, nil)
			i++
		}
	})
}

func BenchmarkSkipList_ConcurrentSearch(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		sl.Insert(key, value, nil)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%10000]
			sl.Search(key)
			i++
		}
	})
}

func BenchmarkSkipList_ConcurrentMixed(b *testing.B) {
	opt := Options{
		MaxLevel:              16,
		P:                     0.25,
		DeleteExpired:         false,
		DeleteExpiredInterval: time.Second,
	}
	sl := NewSkipList(opt)

	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		sl.Insert(key, value, nil)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 16)
		value := make([]byte, 100)
		rand.Read(key)
		rand.Read(value)
		i := 0
		for pb.Next() {
			switch rand.Intn(3) {
			case 0:
				k := append(key, []byte(fmt.Sprintf("%d", i))...)
				sl.Insert(k, value, nil)
			case 1:
				k := keys[i%10000]
				sl.Search(k)
			case 2:
				k := keys[i%10000]
				sl.Delete(k)
			}
			i++
		}
	})
}
