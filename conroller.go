package main

import (
	"GoKeyValueWarehouse/levels/sstable"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LevelsController struct {
	nextFileID atomic.Uint64

	lv []*Level
	db *DB
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

	moveL0toFront := func(prios []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}

	run := func(p compactionPriority) bool {
		err := lc.doCompact(id, p)
		switch err {
		case nil:
			return true
		case errFillTables:
			// pass
		default:
		}
		return false
	}

	var priosBuffer []compactionPriority
	runOnce := func() bool {
		prios := lc.pickCompactLevels(priosBuffer)
		defer func() {
			priosBuffer = prios
		}()
		if id == 0 {
			// Worker ID zero prefers to compact L0 always.
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			if run(p) {
				return true
			}
		}

		return false
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			runOnce()
			// case <-lc.HasBeenClosed():
			// 	return
		}
	}
}

func (s *LevelsController) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < s.db.opt.BaseLevelSize {
			return s.db.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{
		targetSz: make([]int64, len(s.lv)),
		fileSz:   make([]int64, len(s.lv)),
	}
	// DB size is the size of the last level.
	dbSize := s.lastLevel().totalSize
	for i := len(s.lv) - 1; i > 0; i-- {
		ltarget := adjust(dbSize)
		t.targetSz[i] = ltarget
		if t.baseLevel == 0 && ltarget <= s.db.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(s.db.opt.LevelSizeMultiplier)
	}

	tsz := s.db.opt.SSTableOpt.TableSize
	for i := 0; i < len(s.lv); i++ {
		if i == 0 {
			t.fileSz[i] = s.db.opt.MemtableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(s.db.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	for i := t.baseLevel + 1; i < len(s.lv)-1; i++ {
		if s.lv[i].totalSize > 0 {
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
