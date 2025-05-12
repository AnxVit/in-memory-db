package db

import (
	"GoKeyValueWarehouse/sstable"
	"context"
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

func newLevelsController(db *DB, meta *ManifestInfo) (*LevelsController, error) {
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

	for id, level := range meta.Tables {
		fname := sstable.TableName(id, db.opt.Directory)
		select {
		case <-tick.C:
		default:
		}
		if id > maxFileID {
			maxFileID = id
		}
		go func(fname string, lv uint64) {
			defer func() {
				numOpened.Add(1)
			}()

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
		}(fname, level)
	}

	s.nextFileID.Store(maxFileID + 1)
	for i, tbls := range tables {
		s.lv[i].initTables(tbls)
	}

	// if err := s.validate(); err != nil {
	// }

	return s, nil
}

func (lc *LevelsController) startCompact(ctx context.Context, wg *sync.WaitGroup) {
	n := int(lc.db.opt.NumCompactors)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go lc.runCompactor(ctx, wg, i)
	}
}

func (lc *LevelsController) runCompactor(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-ctx.Done():
		randomDelay.Stop()
		return
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
		case <-ctx.Done():
			return
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

	if err := lc.runCompactDef(cd); err != nil {
		return err
	}

	return nil
}

func (s *LevelsController) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		return false
	}

	cd.nextLevel = s.lv[0]
	cd.nextRange = KeyRange{}
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
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			continue
		}
		if _, beingCompacted := s.cstatus.tables[t.GetID()]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		return false
	}
	cd.thisRange = endRange
	cd.top = out

	thisLevel := s.cstatus.levels[cd.thisLevel.lvID]
	thisLevel.ranges = append(thisLevel.ranges, endRange)
	for _, t := range out {
		s.cstatus.tables[t.GetID()] = struct{}{}
	}

	cd.t.fileSize[0] = math.MaxUint32
	return true
}

func (s *LevelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.lvID == 0 {
		panic("Base level can't be zero.")
	}

	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.sst
	if len(top) == 0 {
		return false
	}

	var out []*sstable.SSTable
	var kr KeyRange

	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			break
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(cd.thisRange)
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

func (lc *LevelsController) runCompactDef(cd compactDef) (err error) {
	if len(cd.t.fileSize) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	newTables, decr, err := lc.compactBuildTables(cd)
	if err != nil {
		return err
	}
	defer func() {
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	if err := lc.db.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	return nil
}

func (s *LevelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*sstable.SSTable, len(cd.thisLevel.sst))
	copy(tables, cd.thisLevel.sst)
	if len(tables) == 0 {
		return false
	}
	// if cd.thisLevel.isLastLevel() {
	// 	return s.fillMaxLevelTables(tables, cd)
	// }
	// if len(tables) == 0 || cd.nextLevel == nil {
	// 	sort.Slice(tables, func(i, j int) bool {
	// 		return tables[i].MaxVersion() < tables[j].MaxVersion()
	// 	})
	// }

	for _, t := range tables {
		cd.thisSize = t.GetSize()
		cd.thisRange = getKeyRange(t)

		if s.cstatus.overlapsWith(cd.thisLevel.lvID, cd.thisRange) {
			continue
		}
		cd.top = []*sstable.SSTable{t}
		left, right := cd.nextLevel.overlappingTables(cd.thisRange)

		cd.bot = make([]*sstable.SSTable, right-left)
		copy(cd.bot, cd.nextLevel.sst[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*sstable.SSTable{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(*cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if s.cstatus.overlapsWith(cd.nextLevel.lvID, cd.nextRange) {
			continue
		}
		if !s.cstatus.compareAndAdd(*cd) {
			continue
		}
		return true
	}
	return false
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
	errChan := make(chan error)
	go func(errChan chan<- error) {
		err := ls.db.manifest.addChanges([]*ManifestChange{
			newCreateChange(t.GetID(), 0),
		})
		errChan <- err
	}(errChan)

	ls.lv[0].addLevel0Table(t)
	err := <-errChan
	if err != nil {
		go func() {
			ls.lv[0].deleteLevel0Table()
		}()
	}
	return err
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
