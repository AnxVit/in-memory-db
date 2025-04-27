package main

import (
	"GoKeyValueWarehouse/levels/sstable"
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

func (l *Level) addLevel0Table(t *sstable.SSTable) {
	l.Lock()
	defer l.Unlock()

	l.sst = append(l.sst, t)
	// TODO table mux
	l.totalSize += t.GetSize()
}
