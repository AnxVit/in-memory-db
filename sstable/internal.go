package sstable

func (sst *SSTable) FilterLookup(key []byte) bool {
	return sst.filter.Lookup(key)
}
