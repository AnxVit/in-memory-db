package main

import (
	"GoKeyValueWarehouse/levels/sstable"
	"bytes"
	"sort"
	"sync"
)

type compactDef struct {
	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *Level
	nextLevel   *Level

	top []*sstable.SSTable
	bot []*sstable.SSTable

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64
}

type compactionPriority struct {
	level    int
	score    float64
	adjusted float64
	t        targets
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (cs *compactStatus) compareAndAdd(cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	thisLevel := cs.levels[cd.thisLevel.lvID]
	nextLevel := cs.levels[cd.nextLevel.lvID]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.GetID()] = struct{}{}
	}
	return true
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

type keyRange struct {
	left  []byte
	right []byte
	end   bool
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.end
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || bytes.Compare(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || bytes.Compare(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.end {
		r.end = true
	}
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.isEmpty() {
		return true
	}

	if dst.isEmpty() {
		return false
	}
	if r.end || dst.end {
		return true
	}

	if bytes.Compare(r.left, dst.right) > 0 {
		return false
	}

	if bytes.Compare(r.right, dst.left) < 0 {
		return false
	}

	return true
}

var endRange = keyRange{end: true}

func getKeyRange(tables ...*sstable.SSTable) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	smallest := tables[0].GetSmallest()
	biggest := tables[0].GetBiggest()
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i].GetSmallest(), smallest) < 0 {
			smallest = tables[i].GetSmallest()
		}
		if bytes.Compare(tables[i].GetBiggest(), biggest) > 0 {
			biggest = tables[i].GetBiggest()
		}
	}

	return keyRange{
		left:  smallest,
		right: biggest,
	}
}

func (lc *LevelsController) compactBuildTables(
	lev int, cd compactDef) ([]*sstable.SSTable, func() error, error) {

	topTables := append(cd.top, cd.bot...)

	res := make(chan *sstable.SSTable)
	for _, kr := range cd.splits {
		go func(kr keyRange) {
			it := sstable.NewMergeSSTableIterator(topTables)
			defer it.Close()
			lc.subcompact(it, cd, res)
		}(kr)
	}

	var newTables []*sstable.SSTable
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	close(res)
	wg.Wait()

	sort.Slice(newTables, func(i, j int) bool {
		return bytes.Compare(newTables[i].GetBiggest(), newTables[j].GetBiggest()) < 0
	})
	return newTables, func() error {
		return nil
		//return decrRefs(newTables)
	}, nil
}

func (s *LevelsController) subcompact(it *sstable.MergeSSTableIterator, cd compactDef, res chan<- *sstable.SSTable) {
	var (
		lastKey, skipKey []byte
	)

	addKeys := func() {
		var tableKr keyRange
		for it.HasNext() {

			KV := it.Next()

			if len(skipKey) > 0 {
				if SameKey(KV.Key, skipKey) {
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			if !SameKey(KV.Key, lastKey) {
				// if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
				// 	break
				// }
				// if builder.ReachedCapacity() {
				// 	// Only break if we are on a different key, and have reached capacity. We want
				// 	// to ensure that all versions of the key are stored in the same sstable, and
				// 	// not divided across multiple tables at the same level.
				// 	break
				// }

				lastKey = append(lastKey, KV.Key...)

				if len(tableKr.left) == 0 {
					tableKr.left = append(tableKr.left, KV.Key...)
				}
				tableKr.right = lastKey

			}
			isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)
			switch {
			case isExpired:
				continue
			default:
				//builder.Add(it.Key(), vs, vp.Len)
			}
		}
	}

	// if len(kr.left) > 0 {
	// 	it.Seek(kr.left)
	// } else {
	// 	it.Rewind()
	// }
	for it.Valid() {
		// if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
		// 	break
		// }
		bopts := s.db.opt.SSTableOpt
		addKeys()
		go func(fileID uint64) {
			tbl, err := sstable.CreateTable(fileID, nil, bopts)
			if err != nil {
				return
			}
			res <- tbl
		}(s.getNextFileID())
	}
}

func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(src, dst)
}
