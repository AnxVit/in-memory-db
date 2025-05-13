package db

import (
	"GoKeyValueWarehouse/sstable"
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

	thisRange KeyRange
	nextRange KeyRange

	thisSize int64
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.thisLevel.RUnlock()
	cd.nextLevel.RUnlock()
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

func (cs *compactStatus) overlapsWith(level int, this KeyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	if len(cs.levels) > l {
		return cs.levels[l].delSize
	}
	return 0
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	thisLevel := cs.levels[cd.thisLevel.lvID]
	nextLevel := cs.levels[cd.nextLevel.lvID]

	thisLevel.delSize -= cd.thisSize
	_ = thisLevel.remove(cd.thisRange)

	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		_ = nextLevel.remove(cd.nextRange)
	}

	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.GetID()]
		if ok {
			delete(cs.tables, t.GetID())
		}
	}
}

type levelCompactStatus struct {
	ranges  []KeyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst KeyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (lcs *levelCompactStatus) remove(dst KeyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

type KeyRange struct {
	left  []byte
	right []byte
	end   bool
}

func (r *KeyRange) equals(dst KeyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.end == dst.end
}

func (r *KeyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.end
}

func (r *KeyRange) extend(kr KeyRange) {
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

func (r *KeyRange) overlapsWith(dst KeyRange) bool {
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

var endRange = KeyRange{end: true}

func getKeyRange(tables ...*sstable.SSTable) KeyRange {
	if len(tables) == 0 {
		return KeyRange{}
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

	return KeyRange{
		left:  KeyWithTs(smallest),
		right: KeyWithTs(biggest),
	}
}

func (lc *LevelsController) compactBuildTables(cd compactDef) ([]*sstable.SSTable, func() error, error) {
	topTables := append(cd.top, cd.bot...)

	res := make(chan *sstable.SSTable)
	go func() {
		it := sstable.NewMergeSSTableIterator(topTables)
		defer it.Close()
		lc.subcompact(it, cd, res)
	}()

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

func (lc *LevelsController) subcompact(it *sstable.MergeSSTableIterator, cd compactDef, res chan<- *sstable.SSTable) {
	addKeys := func() {
		for ; it.Vaild(); it.Next() {
			//if bytes.Equal(it.Value(),[]byte(skiplist.TOMBSTONE))
		}
	}
	addKeys()
	go func(fileID uint64) {
		tbl, err := sstable.CreateTable(fileID, nil, lc.db.opt.SSTableOpt)
		if err != nil {
			return
		}
		res <- tbl
	}(lc.getNextFileID())
}

func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(src, dst)
}
