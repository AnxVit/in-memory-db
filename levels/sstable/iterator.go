package sstable

type currentNode struct {
	posIdx   int
	blockIdx int
	key      []byte
	value    []byte
}

type Iterator struct {
	tb      *SSTable
	current *currentNode
}

func (it *Iterator) Next() bool {
	if it.current.blockIdx > len(it.tb.blockIndex)-1 {
		return false
	}

	if it.current.posIdx < len(it.tb.blockIndex[it.current.blockIdx].data) {
		it.current.posIdx += 1
		entry := it.tb.blockIndex[it.current.blockIdx].data[it.current.posIdx]
		it.current.key = entry.Key
		it.current.value = entry.Value
	} else {
		if it.current.blockIdx == len(it.tb.blockIndex)-1 {
			return false
		}
		it.current.blockIdx += 1
		it.current.posIdx = 0
		entry := it.tb.blockIndex[it.current.blockIdx].data[it.current.posIdx]
		it.current.key = entry.Key
		it.current.value = entry.Value
	}
	return true
}

func (it *Iterator) Key() []byte {
	return it.current.key
}

func (it *Iterator) Value() []byte {
	return it.current.value
}

func (it *Iterator) Close() error {
	return nil
}

// Rewind()
// Seek(key []byte)
// Valid() bool
