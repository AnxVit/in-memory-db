package ringcache

import (
	"errors"
	"sync"
)

type RingCache struct {
	maxSize int
	next    int
	keys    []interface{}
	items   map[any]interface{}
	mu      sync.RWMutex
}

func New(maxSize int) (*RingCache, error) {
	if maxSize <= 0 {
		return nil, errors.New("cache size should be greater than zero")
	}
	cache := &RingCache{
		maxSize: maxSize,
		next:    0,
		keys:    make([]interface{}, maxSize),
		items:   make(map[interface{}]interface{}),
	}
	return cache, nil
}

func (c *RingCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[interface{}]interface{})
	c.keys = make([]interface{}, c.maxSize)
	c.next = 0
}

func (c *RingCache) Add(key, value interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key == nil || value == nil {
		return nil, false
	}

	var evicted bool
	if k := c.keys[c.next]; k != nil {
		evicted = true
		delete(c.items, k)
	}

	oldValue := c.items[key]
	c.items[key] = value
	c.keys[c.next] = key
	c.next = (c.next + 1) % c.maxSize
	return oldValue, evicted
}

func (c *RingCache) Get(key interface{}) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if value, ok := c.items[key]; ok {
		return value, ok
	}
	return nil, false
}

func (c *RingCache) Contains(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.items[key]
	return ok
}

func (c *RingCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		delete(c.items, key)
		for i, k := range c.keys {
			if k == key {
				c.keys[i] = nil
				break
			}
		}
		return true
	}
	return false
}

func (c *RingCache) Resize(size int) (evictedKeys []interface{},
	evictedValues []interface{}) {
	if size <= 0 {
		panic("invalid size")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	next := c.next
	for size < len(c.items) {
		if k := c.keys[next]; k != nil {
			evictedKeys = append(evictedKeys, k)
			evictedValues = append(evictedValues, c.items[k])
			delete(c.items, k)
		}
		next = (next + 1) % c.maxSize
	}
	c.maxSize = size
	return evictedKeys, evictedValues
}

func (c *RingCache) GetLeast() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	least := (c.next - 1) % c.maxSize
	if value, ok := c.items[least]; ok {
		return value
	}
	return nil
}
