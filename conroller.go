package main

import (
	"GoKeyValueWarehouse/levels/sstable"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var errFillTables = errors.New("Unable to fill tables")

type LevelsController struct {
	nextFileID atomic.Uint64

	lv []*Level
	db *DB

	cstatus compactStatus
}

func newLevelsController(db *DB, meta *MetaSST) (*LevelsController, error) {
	s := &LevelsController{
		db: db,
		lv: make([]*Level, db.opt.MaxLevels),
	}

	for i := 0; i < db.opt.MaxLevels; i++ {
		s.lv[i] = &Level{
			lvID: i,
			db:   db,
		}
	}

	var mu sync.Mutex
	tables := make([][]*sstable.SSTable, db.opt.MaxLevels)
	var maxFileID uint64

	var numOpened atomic.Int32
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for _, table := range meta.Tables {
		fname := sstable.TableName(table.id, db.opt.Directory)
		select {
		case <-tick.C:
		default:
		}
		if table.id > maxFileID {
			maxFileID = table.id
		}
		go func(fname string, lv int) {
			defer func() {
				numOpened.Add(1)
			}()
			// dk, err := db.registry.DataKey(tf.KeyID)
			// if err != nil {
			// 	rerr = y.Wrapf(err, "Error while reading datakey")
			// 	return
			// }

			file, err := os.Create(fname) // TODO: dir
			if err != nil {
				return
			}
			t, err := sstable.OpenTable(file, db.opt.SSTableOpt)
			if err != nil {
				return
			}

			mu.Lock()
			tables[lv] = append(tables[lv], t)
			mu.Unlock()
		}(fname, table.level)
	}

	s.nextFileID.Store(maxFileID + 1)
	for i, tbls := range tables {
		s.lv[i].initTables(tbls)
	}

	// if err := s.validate(); err != nil {
	// 	_ = s.cleanupLevels()
	// 	return nil, errors.WithMessage(err, "Level validation")
	// }

	return s, nil
}

func (lc *LevelsController) startCompact() {
	n := int(lc.db.opt.NumCompactors)
	for i := 0; i < n; i++ {
		go lc.runCompactor(i)
	}
}

func (lc *LevelsController) runCompactor(id int) {
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	}

	var priosBuffer []compactionPriority
	run := func() {
		prios := lc.setUpCompationPriority(priosBuffer)
		defer func() {
			priosBuffer = prios
		}()
		if id == 0 {
			idx := -1
			for i, p := range prios {
				if p.level == 0 {
					idx = i
					break
				}
			}

			if idx > 0 {
				out := append([]compactionPriority{}, prios[idx])
				out = append(out, prios[:idx]...)
				out = append(out, prios[idx+1:]...)
				prios = out
			}
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			err := lc.doCompact(id, p)
			switch err {
			case nil:
				return
			case errFillTables:
				// log
			default:
			}
			// log.Error
		}

		// log.Error
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			run()
			// case <-lc.HasBeenClosed():
			// 	return
		}
	}
}

func (lc *LevelsController) setUpCompationPriority(priosBuffer []compactionPriority) (prios []compactionPriority) {
	t := lc.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	if cap(priosBuffer) < len(lc.lv) {
		priosBuffer = make([]compactionPriority, 0, len(lc.lv))
	}
	prios = priosBuffer[:0]

	addPriority(0, float64(len(lc.lv[0].sst))/float64(lc.db.opt.NumLevelZeroTables))

	for i := 1; i < len(lc.lv); i++ {
		delSize := lc.cstatus.delSize(i)

		l := lc.lv[i]
		sz := l.totalSize - delSize
		addPriority(i, float64(sz)/float64(t.targetSize[i]))
	}

	var prevLevel int
	for level := t.baseLevel; level < len(lc.lv); level++ {
		if prios[prevLevel].adjusted >= 1 {
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

func (lc *LevelsController) doCompact(id int, p compactionPriority) error {
	l := p.level

	if p.t.baseLevel == 0 {
		p.t = lc.levelTargets()
	}

	cd := compactDef{
		compactorId: id,
		p:           p,
		t:           p.t,
		thisLevel:   lc.lv[l],
	}

	if l == 0 {
		cd.nextLevel = lc.lv[p.t.baseLevel]
		if !lc.fillTablesL0(&cd) {
			return errFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel

		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lc.lv[l+1]
		}
		if !lc.fillTables(&cd) {
			return errFillTables
		}
	}
	defer lc.cstatus.delete(cd) // Remove the ranges from compaction status.

	if err := lc.runCompactDef(id, l, cd); err != nil {
		return err
	}

	return nil
}

func (s *LevelsController) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		return false
	}

	cd.nextLevel = s.lv[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	s.lv[0].RLock()
	defer s.lv[0].RUnlock()

	s.cstatus.Lock()
	defer s.cstatus.Unlock()

	top := cd.thisLevel.sst
	var out []*sstable.SSTable
	now := time.Now()
	for _, t := range top {
		if t.GetSize() >= 2*cd.t.fileSize[0] {
			// This file is already big, don't include it.
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			// Just created it 10s ago. Don't pick for compaction.
			continue
		}
		if _, beingCompacted := s.cstatus.tables[t.GetID()]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// If we don't have enough tables to merge in L0, don't do it.
		return false
	}
	cd.thisRange = endRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	thisLevel := s.cstatus.levels[cd.thisLevel.lvID]
	thisLevel.ranges = append(thisLevel.ranges, endRange)
	for _, t := range out {
		s.cstatus.tables[t.GetID()] = struct{}{}
	}

	// For L0->L0 compaction, we set the target file size to max, so the output is always one file.
	// This significantly decreases the L0 table stalls and improves the performance.
	cd.t.fileSize[0] = math.MaxUint32
	return true
}

func (s *LevelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.lvID == 0 {
		panic("Base level can't be zero.")
	}
	// We keep cd.p.adjusted > 0.0 here to allow functions in db.go to artificially trigger
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.sst
	if len(top) == 0 {
		return false
	}

	var out []*sstable.SSTable
	if len(cd.dropPrefixes) > 0 {
		// Use all tables if drop prefix is set. We don't want to compact only a
		// sub-range. We want to compact all the tables.
		out = top

	} else {
		var kr keyRange
		// cd.top[0] is the oldest file. So we start from the oldest file first.
		for _, t := range top {
			dkr := getKeyRange(t)
			if kr.overlapsWith(dkr) {
				out = append(out, t)
				kr.extend(dkr)
			} else {
				break
			}
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*sstable.SSTable, right-left)
	copy(cd.bot, cd.nextLevel.sst[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return s.cstatus.compareAndAdd(*cd)
}

func (s *LevelsController) fillTablesL0(cd *compactDef) bool {
	if ok := s.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return s.fillTablesL0ToL0(cd)
}

func (lc *LevelsController) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSize) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	if thisLevel.lvID == nextLevel.lvID {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		lc.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables, decr, err := lc.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	// changeSet := buildChangeSet(&cd, newTables)

	// if err := lc.db.manifest.addChanges(changeSet.Changes); err != nil {
	// 	return err
	// }

	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	return nil
}

type targets struct {
	baseLevel  int
	targetSize []int64
	fileSize   []int64
}

func (lc *LevelsController) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < lc.db.opt.BaseLevelSize {
			return lc.db.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{
		targetSize: make([]int64, len(lc.lv)),
		fileSize:   make([]int64, len(lc.lv)),
	}
	// DB size is the size of the last level.
	dbSize := lc.lastLevel().totalSize
	for i := len(lc.lv) - 1; i > 0; i-- {
		ltarget := adjust(dbSize)
		t.targetSize[i] = ltarget
		if t.baseLevel == 0 && ltarget <= lc.db.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lc.db.opt.LevelSizeMultiplier)
	}

	tsz := lc.db.opt.TableSize
	for i := 0; i < len(lc.lv); i++ {
		if i == 0 {
			t.fileSize[i] = lc.db.opt.MemtableSize
		} else if i <= t.baseLevel {
			t.fileSize[i] = tsz
		} else {
			tsz *= int64(lc.db.opt.TableSizeMultiplier)
			t.fileSize[i] = tsz
		}
	}

	for i := t.baseLevel + 1; i < len(lc.lv)-1; i++ {
		if lc.lv[i].totalSize > 0 {
			break
		}
		t.baseLevel = i
	}

	return t
}

func (ls *LevelsController) addLevel0Table(t *sstable.SSTable) error {
	go func() {
		// err := ls.db.manifest.addChanges([]*pb.ManifestChange{
		// 	newCreateChange(t.ID(), 0, t.KeyID(), t.CompressionType()),
		// })
		// if err != nil {
		// 	return err
		// }
	}()

	ls.lv[0].addLevel0Table(t)

	return nil
}

func (lc *LevelsController) lastLevel() *Level {
	return lc.lv[len(lc.lv)-1]
}

func (lc *LevelsController) getNextFileID() uint64 {
	id := lc.nextFileID.Add(1)
	return id - 1
}

func (lc *LevelsController) get(_ []byte) (interface{}, error) {
	return nil, nil
}
