package sstable

import (
	"bytes"
	"container/heap"
)

type KV struct {
	Key   []byte
	Value []byte
}

type item struct {
	kv    KV
	srcID int // index of iterator
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
	visited map[string]bool
}

func NewMergeSSTableIterator(tables []*SSTable) *MergeSSTableIterator {
	it := &MergeSSTableIterator{
		tables:  tables,
		visited: make(map[string]bool),
	}

	for tableIdx, table := range tables {
		heap.Push(&it.pq, &item{
			kv: KV{
				Key:   table.blockIndex[0].data[0].Key,
				Value: table.blockIndex[0].data[0].Value,
			},
			srcID: tableIdx,
			index: [2]int{0, 0},
		})
	}
	heap.Init(&it.pq)
	return it
}

func (it *MergeSSTableIterator) HasNext() bool {
	for len(it.pq) > 0 {
		top := heap.Pop(&it.pq).(*item)
		key := top.kv.Key

		if !it.visited[string(key)] {
			it.visited[string(key)] = true
			heap.Push(&it.pq, top)
			return true
		}

		var nextIdx [2]int
		entries := it.tables[top.srcID].blockIndex[top.index[0]].data
		if len(entries) > top.index[1]+1 {
			nextIdx = [2]int{top.index[0], top.index[1] + 1}
		} else {
			if len(it.tables[top.srcID].blockIndex) > top.index[0]+1 {
				nextIdx = [2]int{top.index[0] + 1, 0}
			} else {
				continue
			}
		}

		heap.Push(&it.pq, &item{
			kv: KV{
				Key:   it.tables[top.srcID].blockIndex[nextIdx[0]].data[nextIdx[1]].Key,
				Value: it.tables[top.srcID].blockIndex[nextIdx[0]].data[nextIdx[1]].Value,
			},
			srcID: top.srcID,
			index: nextIdx,
		})
	}
	return false
}

func (it *MergeSSTableIterator) Next() KV {
	if !it.HasNext() {
		panic("No more elements")
	}
	top := heap.Pop(&it.pq).(*item)

	var nextIdx [2]int
	entries := it.tables[top.srcID].blockIndex[top.index[0]].data
	if len(entries) > top.index[1]+1 {
		nextIdx = [2]int{top.index[0], top.index[1] + 1}
	} else {
		if len(it.tables[top.srcID].blockIndex) > top.index[0]+1 {
			nextIdx = [2]int{top.index[0] + 1, 0}
		} else {
			return top.kv
		}
	}
	heap.Push(&it.pq, &item{
		kv: KV{
			Key:   it.tables[top.srcID].blockIndex[nextIdx[0]].data[nextIdx[1]].Key,
			Value: it.tables[top.srcID].blockIndex[nextIdx[0]].data[nextIdx[1]].Value,
		},
		srcID: top.srcID,
		index: nextIdx,
	})

	return top.kv
}

func (it *MergeSSTableIterator) Close() {

}
