package main

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/wal"
)

type memtable struct {
	sl    *skiplist.SkipList
	wal   *wal.Log
	index uint64

	opt Options
}

func (db *DB) newMemtable() (*memtable, error) {
	m := &memtable{
		sl:  skiplist.NewSkipList(db.opt.MemtableOpt),
		opt: db.opt,
	}

	wal, err := wal.Open(&db.opt.WALOpt)
	if err != nil {
		return nil, err
	}

	m.wal = wal
	return m, nil
}

func (mt *memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *memtable) Put(entry *Entry) error {
	if mt.wal != nil {
		data := serializeOp(entry.Key, entry.Value)
		if err := mt.wal.Write(mt.index, data); err != nil {
			return err
		}
	}

	mt.sl.Insert(entry.Key, entry.Value, nil)

	return nil
}

func (mt *memtable) isFull() bool {
	if mt.sl.Size() > int(mt.opt.MemtableOpt.MemtableFlushThreshold) {
		return true
	}
	return false
}
