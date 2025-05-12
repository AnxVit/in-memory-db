package skiplist

import (
	"bytes"
	"encoding/json"
	"time"
)

type Iterator interface {
	Next() bool                                // Move to the next node
	Prev() bool                                // Move to the previous node
	HasNext() bool                             // Check if there is a next node
	HasPrev() bool                             // Check if there is a previous node
	Current() ([]byte, []byte, *time.Duration) // Get the current key and value
}

type SkipListIterator struct {
	skipList *SkipList
	current  *Node
}

func NewIterator(sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		skipList: sl,
		current:  sl.header,
	}
}

func (it *SkipListIterator) Next() bool {
	for it.current.lv.nodes[0] != nil {
		it.current = it.current.lv.nodes[0]
		if !it.current.IsExpired() {
			return true
		}
	}
	return false
}

func (it *SkipListIterator) Prev() bool {
	current := it.skipList.header
	for i := it.skipList.h; i >= 0; i-- {
		for current.lv.nodes[i] != nil && bytes.Compare(current.lv.nodes[i].key, it.current.key) < 0 {
			if current.lv.nodes[i].IsExpired() {
			}
			current = current.lv.nodes[i]
		}
	}
	if current != it.skipList.header {
		it.current = current
		return true
	}
	return false

}

func (it *SkipListIterator) HasNext() bool {
	next := it.current.lv.nodes[0]
	for next != nil && next.IsExpired() {
		it.skipList.Delete(next.key)
		next = it.current.lv.nodes[0]
	}
	return next != nil
}

func (it *SkipListIterator) HasPrev() bool {
	return it.current != it.skipList.header
}

func (it *SkipListIterator) Current() ([]byte, []byte, *time.Duration) {
	if it.current == it.skipList.header || it.current.IsExpired() {
		if it.current.IsExpired() {
		}
		return nil, nil, nil
	}
	value, _ := json.Marshal(it.current.value)
	return it.current.key, value, timeToDuration(it.current.ttl)
}
