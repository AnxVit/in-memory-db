package db

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/wal"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type memtable struct {
	sl    *skiplist.SkipList
	wal   *wal.Log
	index uint64

	opt Options
}

func (db *DB) openMemtables() error {
	dirs, err := os.ReadDir(db.opt.WALOpt.Directory)
	if err != nil {
		return errors.WithMessagef(err, "unable to open mem dir: %s", db.opt.WALOpt.Directory)
	}

	var fids []int
	for _, dir := range dirs {
		fid, err := strconv.ParseInt(dir.Name(), 10, 64)
		if err != nil {
			return errors.WithMessagef(err, "unable to parse log id: %s", dir.Name())
		}
		fids = append(fids, int(fid))
	}

	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	wg := new(errgroup.Group)
	db.flushQueue = make([]*memtable, len(fids))
	for i, fid := range fids[:len(fids)-1] {
		wg.Go(func() error {
			mt, err := db.newMemtable(fid)
			if err != nil {
				return errors.WithMessagef(err, "while opening fid: %d", fid)
			}
			// if mt.sl.Empty() {
			// 	mt.DecrRef()
			// 	continue
			// }
			db.flushQueue[i] = mt
			return nil
		})
	}
	if len(fids) != 0 {
		db.memtableNextID = fids[len(fids)-1]
	}

	return wg.Wait()
}

func (db *DB) newMemtable(fid int) (*memtable, error) {
	memDirPath := fmt.Sprintf("%05d", fid)
	m := &memtable{
		sl:  skiplist.NewSkipList(db.opt.SkipListOpt),
		opt: db.opt,
	}

	wal, err := wal.Open(&db.opt.WALOpt, memDirPath)
	if err != nil {
		return nil, err
	}
	m.wal = wal

	if err = m.UpdateSkipList(); err != nil {
		return nil, err
	}
	return m, err
}

func (mt *memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *memtable) Put(entry *Entry) error {
	entry.Key = KeyWithTs(entry.Key)
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
	return mt.sl.Size() > int(mt.opt.SkipListOpt.MemtableFlushThreshold)
}

func (mt *memtable) UpdateSkipList() error {
	for idx := mt.wal.GetFirstIndex(); idx < mt.wal.GetLastIndex(); idx++ {
		data, err := mt.wal.Read(idx)
		if err != nil {
			return errors.WithMessagef(err, "failed to read WAL at index: %d", idx)
		}

		entry, err := desialaizeOp(data)
		if err != nil {
			return err
		}

		mt.sl.Insert(entry.Key, entry.Value, nil)
	}
	return nil
}
