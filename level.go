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

func (s *Level) replaceTables(toDel, toAdd []*sstable.SSTable) error {
	s.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.GetID()] = struct{}{}
	}
	var newTables []*sstable.SSTable
	for _, t := range s.sst {
		_, found := toDelMap[t.GetID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.totalSize -= t.GetSize()
	}

	for _, t := range toAdd {
		s.totalSize += t.GetSize()
		// t.IncrRef()
		newTables = append(newTables, t)
	}

	s.sst = newTables
	sort.Slice(s.sst, func(i, j int) bool {
		return bytes.Compare(s.sst[i].GetSmallest(), s.sst[j].GetSmallest()) < 0
	})
	s.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return nil // decrRefs(toDel)
}

func (s *Level) deleteTables(toDel []*sstable.SSTable) error {
	s.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.GetID()] = struct{}{}
	}

	var newTables []*sstable.SSTable
	for _, t := range s.sst {
		_, found := toDelMap[t.GetID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.totalSize -= t.GetSize()
	}
	s.sst = newTables

	s.Unlock()

	return nil // decrRefs(toDel)
}

func (s *Level) overlappingTables(kr KeyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(s.sst), func(i int) bool {
		return bytes.Compare(kr.left, s.sst[i].GetBiggest()) <= 0
	})
	right := sort.Search(len(s.sst), func(i int) bool {
		return bytes.Compare(kr.right, s.sst[i].GetSmallest()) < 0
	})
	return left, right
}

func (s *Level) isLastLevel() bool {
	return s.lvID == s.db.opt.MaxLevels-1
}
