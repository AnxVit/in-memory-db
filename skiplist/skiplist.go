package skiplist

import (
	"GoKeyValueWarehouse/skiplist/debounce"
	"bytes"
	"context"
	"math/rand/v2"
	"sync"
	"time"
	"unsafe"
)

type Node struct {
	key     []byte
	value   interface{}
	lv      *Levels
	ttl     *time.Time
	expired bool
}

type Levels struct {
	nodes []*Node
}

type SkipList struct {
	opt Options

	header   *Node
	h        int // height
	size     int // size
	maxLevel int
	p        float64 // probability

	debouncer map[string]*debounce.Debouncer
	mu        sync.RWMutex
}

func NewNode(level int, key []byte, value interface{}, ttl *time.Duration) *Node {
	var expiration *time.Time
	if ttl != nil {
		exp := time.Now().Add(*ttl)
		expiration = &exp
	}
	return &Node{
		key:   key,
		value: value,
		lv: &Levels{
			nodes: make([]*Node, level+1),
		},
		ttl: expiration,
	}
}

func (n *Node) Size() int {
	return int(unsafe.Sizeof(*n)) + len(n.key) + int(unsafe.Sizeof(n.value))
}

func (n *Node) IsExpired() bool {
	if n.ttl == nil {
		return false
	}
	if n.expired {
		return true
	}
	return time.Now().After(*n.ttl)
}

func NewSkipList(opt Options) *SkipList {
	sk := &SkipList{
		opt:       opt,
		header:    NewNode(int(opt.MemtableMaxLevel), nil, nil, nil),
		h:         0,
		size:      0,
		maxLevel:  int(opt.MemtableMaxLevel),
		p:         opt.MemtableP,
		debouncer: make(map[string]*debounce.Debouncer),
	}
	go sk.worker(context.TODO())

	return sk
}

func (sl *SkipList) Size() int {
	return sl.size
}

func (sl *SkipList) Copy() *SkipList {
	newSkipList := NewSkipList(sl.opt)
	it := NewIterator(sl)
	for it.Next() {
		key, value, ttl := it.Current()
		if value != nil && ttl != nil {
			newSkipList.Insert(key, value, ttl)
		}
	}
	return newSkipList
}

func (sl *SkipList) randomLevel() int {
	level := 0
	for rand.Float64() < sl.p && level < sl.maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Insert(key []byte, value interface{}, ttl *time.Duration) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	update := make([]*Node, sl.maxLevel+1)
	current := sl.header

	for i := sl.h; i >= 0; i-- {
		for current.lv.nodes[i] != nil && (bytes.Compare(current.lv.nodes[i].key, key) < 0 || current.lv.nodes[i].expired) {
			current = current.lv.nodes[i]
		}
		update[i] = current
	}

	current = current.lv.nodes[0]
	if current != nil && bytes.Equal(current.key, key) {
		sl.size -= current.Size()
		current.value = value
		current.ttl = nil
		if ttl != nil {
			if _, ok := sl.debouncer[string(key)]; !ok {
				sl.debouncer[string(key)] = debounce.New(*ttl, func() {
					current.expired = true
				})
			}
			sl.debouncer[string(key)].Trigger()
			exp := time.Now().Add(*ttl)
			current.ttl = &exp
		} else {
			if _, ok := sl.debouncer[string(key)]; ok {
				sl.debouncer[string(key)].Stop()
			}
		}
		current.expired = false
		sl.size += current.Size()
		return
	}

	level := sl.randomLevel()
	if level > sl.h {
		for i := sl.h + 1; i <= level; i++ {
			update[i] = sl.header
		}
		sl.h = level
	}

	newNode := NewNode(level, key, value, ttl)
	for i := 0; i <= level; i++ {
		newNode.lv.nodes[i] = update[i].lv.nodes[i]
		update[i].lv.nodes[i] = newNode
	}
	if ttl != nil {
		sl.debouncer[string(key)] = debounce.New(*ttl, func() {
			newNode.expired = true
		})
		sl.debouncer[string(key)].Trigger()
	}

	sl.size += newNode.Size()
}

func (sl *SkipList) Search(key []byte) (interface{}, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	current := sl.header

	for i := sl.h; i >= 0; i-- {
		for current.lv.nodes[i] != nil && (bytes.Compare(current.lv.nodes[i].key, key) < 0 || current.lv.nodes[i].expired) {
			current = current.lv.nodes[i]
		}
	}

	current = current.lv.nodes[0]
	if current != nil && bytes.Equal(current.key, key) {
		if current.IsExpired() {
			return nil, false
		}
		return current.value, true
	}
	return nil, false
}

func timeToDuration(t *time.Time) *time.Duration {
	if t == nil {
		return nil
	}

	duration := time.Until(*t)
	return &duration

}

func (sl *SkipList) Delete(key []byte) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	update := make([]*Node, sl.maxLevel+1)
	current := sl.header

	for i := sl.h; i >= 0; i-- {
		for current.lv.nodes[i] != nil && (bytes.Compare(current.lv.nodes[i].key, key) < 0 || current.lv.nodes[i].expired) {
			current = current.lv.nodes[i]
		}
		update[i] = current
	}

	current = current.lv.nodes[0]
	if current != nil && bytes.Equal(current.key, key) {
		for i := 0; i <= sl.h; i++ {
			if update[i].lv.nodes[i] != current {
				break
			}
			update[i].lv.nodes[i] = current.lv.nodes[i]
		}

		sl.size -= current.Size()

		for sl.h > 0 && sl.header.lv.nodes[sl.h] == nil {
			sl.h--
		}
	}
}

func (sl *SkipList) worker(ctx context.Context) {
	ticker := time.NewTicker(sl.opt.DeleteExpiredInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			sl.mu.Lock()
			sl.deleteExpired()
			sl.mu.Unlock()
		}
	}
}

func (sl *SkipList) deleteExpired() {
	update := make([]*Node, sl.maxLevel+1)
	current := sl.header
	for {
		for i := sl.h; i >= 0; i-- {
			for current.lv.nodes[i] != nil && !current.lv.nodes[i].IsExpired() {
				current = current.lv.nodes[i]
			}
			update[i] = current
		}

		current = current.lv.nodes[0]
		if current != nil && current.IsExpired() {
			for i := 0; i <= sl.h; i++ {
				if update[i].lv.nodes[i] != current {
					break
				}
				update[i].lv.nodes[i] = current.lv.nodes[i]
			}

			sl.size -= current.Size()

			for sl.h > 0 && sl.header.lv.nodes[sl.h] == nil {
				sl.h--
			}
		} else {
			return
		}
	}
}
