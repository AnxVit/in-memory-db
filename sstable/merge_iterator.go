package sstable

import (
	"bytes"
	"container/heap"
	"time"
)

type KV struct {
	Key     []byte
	Version time.Time
	Value   []byte
}

type item struct {
	kv    KV
	srcID int // index of table
	index [2]int
}

type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	if bytes.Equal(pq[i].kv.Key, pq[j].kv.Key) {
		return pq[i].srcID < pq[j].srcID
	}
	return bytes.Compare(pq[i].kv.Key, pq[j].kv.Key) < 0
}
func (pq priorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*item)) }
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	top := old[n-1]
	*pq = old[:n-1]
	return top
}

type MergeSSTableIterator struct {
	tables  []*SSTable
	pq      priorityQueue
	current KV
	visited map[string]bool
}

func NewMergeSSTableIterator(tables []*SSTable) *MergeSSTableIterator {
	it := &MergeSSTableIterator{
		tables:  tables,
		visited: make(map[string]bool),
	}

	for tableIdx, table := range tables {
		entry, err := table.searchInBlock(table.blockIndex[0].smallest, table.blockIndex[0])
		if err != nil {
			// log
		}
		heap.Push(&it.pq, &item{
			kv: KV{
				Key:   entry.Key,
				Value: entry.Value,
			},
			srcID: tableIdx,
			index: [2]int{0, 0},
		})
	}
	heap.Init(&it.pq)
	return it
}

func (it *MergeSSTableIterator) Vaild() bool {
	for len(it.pq) > 0 {
		top := heap.Pop(&it.pq).(*item)
		key := top.kv.Key

		if !it.visited[string(key)] {
			it.visited[string(key)] = true
			heap.Push(&it.pq, top)
			return true
		}

		var (
			nextIdx     [2]int
			isNextBlock bool
		)
		block := it.tables[top.srcID].blockIndex[top.index[0]]
		entries, err := it.tables[top.srcID].getEntriesFromBlock(block)
		if err != nil {
			// log
			return false
		}
		if len(entries) > top.index[1]+1 {
			nextIdx = [2]int{top.index[0], top.index[1] + 1}
		} else {
			if len(it.tables[top.srcID].blockIndex) > top.index[0]+1 {
				isNextBlock = true
				nextIdx = [2]int{top.index[0] + 1, 0}
			} else {
				continue
			}
		}

		if isNextBlock {
			block := it.tables[top.srcID].blockIndex[nextIdx[0]]
			entries, err = it.tables[top.srcID].getEntriesFromBlock(block)
			if err != nil {
				// log
				return false
			}
		}

		heap.Push(&it.pq, &item{
			kv: KV{
				Key:   entries[nextIdx[1]].Key,
				Value: entries[nextIdx[1]].Value,
			},
			srcID: top.srcID,
			index: nextIdx,
		})
	}
	return false
}

func (it *MergeSSTableIterator) Next() {
	top := heap.Pop(&it.pq).(*item)

	var (
		nextIdx     [2]int
		isNextBlock bool
	)
	block := it.tables[top.srcID].blockIndex[top.index[0]]
	entries, err := it.tables[top.srcID].getEntriesFromBlock(block)
	if err != nil {
		// log
	}
	if len(entries) > top.index[1]+1 {
		nextIdx = [2]int{top.index[0], top.index[1] + 1}
	} else {
		if len(it.tables[top.srcID].blockIndex) > top.index[0]+1 {
			isNextBlock = true
			nextIdx = [2]int{top.index[0] + 1, 0}
		} else {
			it.current = top.kv
		}
	}
	if isNextBlock {
		block := it.tables[top.srcID].blockIndex[nextIdx[0]]
		entries, err = it.tables[top.srcID].getEntriesFromBlock(block)
		if err != nil {
			// log
			return
		}
	}
	heap.Push(&it.pq, &item{
		kv: KV{
			Key:   entries[nextIdx[1]].Key,
			Value: entries[nextIdx[1]].Value,
		},
		srcID: top.srcID,
		index: nextIdx,
	})

	it.current = top.kv
}

func (it *MergeSSTableIterator) Key() []byte {
	return it.current.Key
}

func (it *MergeSSTableIterator) Value() []byte {
	return it.current.Value
}

func (it *MergeSSTableIterator) Close() {

}
