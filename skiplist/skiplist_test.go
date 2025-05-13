package skiplist

import (
	"fmt"
	"testing"
	"time"
)

func TestNewSkipList(t *testing.T) {
	sl := NewSkipList(Options{
		DeleteExpiredInterval: 1 * time.Second,
		MaxLevel:              12,
		P:                     0.25,
	})
	if sl == nil {
		t.Fatal("Expected new skip list to be created")
	}
	if sl.h != 0 {
		t.Fatalf("Expected level to be 0, got %d", sl.h)
	}
	if sl.size != 0 {
		t.Fatalf("Expected size to be 0, got %d", sl.size)
	}
}

func TestInsertAndSearch(t *testing.T) {
	sl := NewSkipList(Options{
		DeleteExpiredInterval: 1 * time.Second,
		MaxLevel:              12,
		P:                     0.25,
	})
	key := []byte("key1")
	value := []byte("value1")
	sl.Insert(key, value, nil)

	val, found := sl.Search(key)
	if !found {
		t.Fatal("Expected to find the key")
	}

	if string(val.([]byte)) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, val)
	}
}

func Test_MultiInsert(t *testing.T) {
	sl := NewSkipList(Options{
		DeleteExpiredInterval: 1 * time.Second,
		MaxLevel:              16,
		P:                     0.5,
	})
	for i := range 10 {
		key := []byte(fmt.Sprintf("%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		sl.Insert(key, value, nil)
	}

	key := []byte("1")
	value := "value1"

	val, found := sl.Search(key)
	if !found {
		t.Fatal("Expected to find the key")
	}

	if val.(string) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, val)
	}
}

func TestExp(t *testing.T) {
	sl := NewSkipList(Options{
		DeleteExpiredInterval: 1 * time.Second,
		MaxLevel:              12,
		P:                     0.25,
	})
	key := []byte("key1")
	value := []byte("value1")
	ttl := time.Second
	sl.Insert(key, value, &ttl)

	time.Sleep(time.Second)

	_, found := sl.Search(key)
	if found {
		t.Fatal("Expected don't find the key")
	}
}

func TestExpSkip(t *testing.T) {
	sl := NewSkipList(Options{
		DeleteExpiredInterval: 1 * time.Second,
		MaxLevel:              12,
		P:                     0.25,
	})
	key := []byte("key1")
	value := []byte("value1")
	upValue := []byte("value2")
	ttl := time.Second
	sl.Insert(key, value, &ttl)

	sl.Insert(key, upValue, nil)

	time.Sleep(time.Second)

	val, found := sl.Search(key)
	if !found {
		t.Fatal("Expected to find the key")
	}

	if val.(string) != string(upValue) {
		t.Fatalf("Expected value %s, got %s", upValue, val)
	}
}
