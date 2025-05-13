package db

import (
	"GoKeyValueWarehouse/sstable"
	"bytes"
	"sort"
	"sync"
)

type Level struct {
	sync.RWMutex

	sst []*sstable.SSTable

	lvID      int
	totalSize int64

	db *DB
}

func (s *Level) initTables(tables []*sstable.SSTable) {
	s.Lock()
	defer s.Unlock()

	s.sst = tables
	s.totalSize = 0
	for _, t := range tables {
		s.totalSize += t.GetSize()
	}

	if s.lvID == 0 {
		sort.Slice(s.sst, func(i, j int) bool {
			return s.sst[i].GetID() < s.sst[j].GetID()
		})
	} else {
		sort.Slice(s.sst, func(i, j int) bool {
			return bytes.Compare(s.sst[i].GetSmallest(), s.sst[j].GetSmallest()) < 0
		})
	}
}

func (l *Level) addLevel0Table(t *sstable.SSTable) int {
	l.Lock()
	defer l.Unlock()

	l.sst = append(l.sst, t)
	// TODO table mux
	l.totalSize += t.GetSize()

	return len(l.sst)
}

func (l *Level) deleteLevel0Table() {
	l.Lock()
	defer l.Unlock()

	t := l.sst[len(l.sst)-1]
	l.sst = l.sst[:len(l.sst)-1]
	// TODO table mux
	l.totalSize -= t.GetSize()
}

func (l *Level) replaceTables(toDel, toAdd []*sstable.SSTable) error {
	l.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.GetID()] = struct{}{}
	}
	var newTables []*sstable.SSTable
	for _, t := range l.sst {
		_, found := toDelMap[t.GetID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		l.totalSize -= t.GetSize()
	}

	for _, t := range toAdd {
		l.totalSize += t.GetSize()
		// t.IncrRef()
		newTables = append(newTables, t)
	}

	l.sst = newTables
	sort.Slice(l.sst, func(i, j int) bool {
		return bytes.Compare(l.sst[i].GetSmallest(), l.sst[j].GetSmallest()) < 0
	})
	l.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return nil // decrRefs(toDel)
}

func (l *Level) deleteTables(toDel []*sstable.SSTable) error {
	l.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.GetID()] = struct{}{}
	}

	var newTables []*sstable.SSTable
	for _, t := range l.sst {
		_, found := toDelMap[t.GetID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		l.totalSize -= t.GetSize()
	}
	l.sst = newTables

	l.Unlock()

	return nil // decrRefs(toDel)
}

func (l *Level) overlappingTables(kr KeyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(l.sst), func(i int) bool {
		return bytes.Compare(kr.left, l.sst[i].GetBiggest()) <= 0
	})
	right := sort.Search(len(l.sst), func(i int) bool {
		return bytes.Compare(kr.right, l.sst[i].GetSmallest()) < 0
	})
	return left, right
}

func (l *Level) get(key []byte) (*sstable.Entry, uint64, error) {
	if l.lvID == 0 {
		var maxVersion uint64
		var maxEntry *sstable.Entry
		for _, table := range l.sst {
			if table.FilterLookup(key) {
				entry, err := table.Search(key)
				if err != nil {
					// log
					continue
				}
				if maxVersion < ParseTs(entry.Key) {
					maxVersion = table.GetID()
					maxEntry = entry
				}
			}
		}
		return maxEntry, maxVersion, nil
	}

	for _, table := range l.sst {
		if table.FilterLookup(key) {
			vs := table.GetID()
			entry, err := table.Search(key)
			return entry, vs, err
		}
	}

	return &sstable.Entry{}, 0, nil
}

func (l *Level) isLastLevel() bool {
	return l.lvID == l.db.opt.MaxLevels-1
}

func (l *Level) close() {
	for _, table := range l.sst {
		table.Close()
	}
}
