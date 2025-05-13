package sstable

import (
	"bytes"

	"github.com/pkg/errors"
)

func (sst *SSTable) FilterLookup(key []byte) bool {
	return sst.filter.Lookup(key)
}

func (sst *SSTable) Search(key []byte) (*Entry, error) {
	var (
		block BlockIndex
		ok    bool
	)
	for i := 0; i < len(sst.blockIndex); i++ {
		if bytes.Compare(sst.blockIndex[i].smallest, key) <= 0 && bytes.Compare(sst.blockIndex[i].biggest, key) >= 0 {
			ok = true
			block = sst.blockIndex[i]
			break
		}
	}
	if ok {
		sst.muCache.RLock()
		if entries, ok := sst.blockCache.Get(block.offset); ok {
			sst.muCache.RUnlock()
			for _, entry := range entries {
				if bytes.Equal(key, entry.Key) {
					return &entry, nil
				}
			}
		} else {
			go func() {
				sst.muCache.Lock()
				defer sst.muCache.Unlock()
				entries, err := sst.getEntriesFromBlock(block)
				if err != nil {
					return
				}
				sst.blockCache.Add(block.offset, entries)
			}()
		}

		return sst.searchInBlock(key, block)
	}
	return nil, errors.New("not found key in sst")
}
