package db

import (
	"GoKeyValueWarehouse/skiplist"
	"GoKeyValueWarehouse/sstable"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	kvWriteChCapacity = 100
)

type CancelComponents struct {
	writer     context.CancelFunc
	compaction context.CancelFunc
}

func (c *CancelComponents) Do() {
	c.writer()
	c.compaction()
}

var cancelComponents = &CancelComponents{}

type DB struct {
	memtable       *memtable
	memtableLock   *sync.RWMutex
	memtableNextID int

	lc       *LevelsController
	manifest *manifest

	flushQueue     []*memtable
	flushQueueChan chan *memtable

	transactionsLock *sync.RWMutex
	transactions     []*Tranzaction

	isClosed atomic.Uint32

	writeCh chan *request
	wg      *sync.WaitGroup
	lock    *sync.RWMutex

	opt       Options
	closeOnce sync.Once
	exit      chan struct{}
}

func Open(opts Options) (*DB, error) {
	err := os.MkdirAll(opts.Directory, 0755)
	if err != nil {
		return nil, err
	}

	db := &DB{
		memtableLock: &sync.RWMutex{},

		flushQueue: make([]*memtable, opts.NumImmutable),

		transactionsLock: &sync.RWMutex{},
		transactions:     make([]*Tranzaction, 0),

		writeCh: make(chan *request, kvWriteChCapacity),
		wg:      &sync.WaitGroup{},

		opt:  opts,
		exit: make(chan struct{}),
	}

	err = db.openMemtables()
	if err != nil {
		return nil, err
	}

	db.memtable, err = db.newMemtable(db.memtableNextID)
	if err != nil {
		return nil, err
	}

	db.manifest, err = NewManifest(opts.ManifestDir)
	if err != nil {
		return nil, err
	}

	db.lc, err = newLevelsController(db, &db.manifest.manifestInfo)
	if err != nil {
		return nil, err
	}

	db.wg.Add(1)
	ctxWriter, cancel := context.WithCancel(context.Background())
	go db.backgroundWriter(ctxWriter, db.wg) // start the background writer
	cancelComponents.writer = cancel

	db.wg.Add(1)
	go db.flushMemtable(db.wg) // flush memtable

	db.wg.Add(1)
	ctxCompaction, cancel := context.WithCancel(context.Background())
	go db.startCompactions(ctxCompaction, db.wg) // start the background compactor
	cancelComponents.compaction = cancel

	return db, nil
}

// TODO: change wg wait
func (db *DB) close() error {
	db.isClosed.Store(1)

	if db.memtable.sl.Size() > 0 {
		// db.appendMemtableToFlushQueue()
	}

	cancelComponents.Do()

	close(db.exit)
	close(db.writeCh)

	db.wg.Wait()

	// for _, sstable := range db.sstables {
	// 	err := sstable.Close()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	if db.memtable.wal != nil {
		close(db.flushQueueChan)
		err := db.memtable.wal.Close()
		if err != nil {
			return err
		}
	}

	db.manifest.close()

	return nil
}

func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.close()
	})
	return err
}

func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) backgroundWriter(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	semaphore := make(chan struct{}, 1)

	handleBatch := func(batch []*request) {
		go func(reqs []*request) {
			defer func() { <-semaphore }()
			_ = db.writeRequests(reqs)
		}(batch)
	}

	var (
		reqs = make([]*request, 0, kvWriteChCapacity)
		r    *request
	)

	for {
		select {
		case r = <-db.writeCh:
		case <-ctx.Done():
			goto doneLoop
		}

	collectLoop:
		for {
			reqs = append(reqs, r)

			if len(reqs) >= kvWriteChCapacity {
				semaphore <- struct{}{}
				handleBatch(reqs)
				reqs = make([]*request, 0, kvWriteChCapacity)
				break collectLoop
			}

			select {
			case r = <-db.writeCh:
			case semaphore <- struct{}{}:
				handleBatch(reqs)
				reqs = make([]*request, 0, kvWriteChCapacity)
				break collectLoop
			case <-ctx.Done():
				goto doneLoop
			}
		}

		if ctx.Err() != nil {
			goto doneLoop
		}
	}

doneLoop:
	for {
		select {
		case r = <-db.writeCh:
			reqs = append(reqs, r)
		default:
			if len(reqs) > 0 {
				semaphore <- struct{}{}
				handleBatch(reqs)
			}
			return
		}
	}
}

func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		var i uint64
		var err error
		for err = db.checkFlushQueue(); err == ErrorsWait; err = db.checkFlushQueue() {
			i++
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			return errors.WithMessage(err, "writeRequests")
		}
		if err := db.writeToLSM(b.Entries); err != nil {
			return errors.WithMessage(err, "writeRequests")
		}
	}

	return nil
}

func (db *DB) writeToLSM(b []*Entry) error {
	for _, entry := range b {
		err := db.memtable.Put(entry)
		if err != nil {
			return errors.WithMessage(err, "while writing to memTable")
		}
	}
	return db.memtable.SyncWAL()
}

func (db *DB) checkFlushQueue() error {
	var err error
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.memtable.isFull() {
		return nil
	}

	select {
	case db.flushQueueChan <- db.memtable:
		db.flushQueue = append(db.flushQueue, db.memtable)
		db.memtable, err = db.newMemtable(db.memtableNextID)
		if err != nil {
			return errors.WithMessage(err, "cannot create new mem table")
		}

		return nil
	default:
		return ErrorsWait
	}
}

func (db *DB) flushMemtable(wg *sync.WaitGroup) {
	defer wg.Done()

	for mt := range db.flushQueueChan {
		if mt == nil {
			continue
		}

		for {
			if err := db.flushMemtableToDisk(mt); err != nil {
				time.Sleep(time.Second)
				continue
			}

			db.lock.Lock()
			db.flushQueue = db.flushQueue[1:]
			// mt.DecrRef() // Return memory.
			db.lock.Unlock()
			break
		}
	}
}

func (db *DB) startCompactions(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	db.lc.startCompact(ctx, wg)
}

func (db *DB) flushMemtableToDisk(mt *memtable) error {
	iter := skiplist.NewIterator(mt.sl)
	fileID := db.lc.getNextFileID()
	tbl, err := sstable.CreateTable(fileID, iter, db.opt.SSTableOpt)
	if err != nil {
		return errors.WithMessage(err, "could not flush memtable to disk")
	}
	return db.lc.addLevel0Table(tbl)
}

func (db *DB) getMemTables() ([]*memtable, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*memtable
	tables = append(tables, db.memtable)
	// db.memtable.IncrRef()

	last := len(db.flushQueue) - 1
	for i := range db.flushQueue {
		tables = append(tables, db.flushQueue[last-i])
		// db.flushQueue[last-i].IncrRef()
	}
	return tables, func() {
		// for _, tbl := range tables {
		// 		tbl.DecrRef()
		// }
	}
}

func (db *DB) get(key []byte) (interface{}, error) {
	if db.IsClosed() {
		return nil, ErrorClosedDB
	}
	mts, decr := db.getMemTables()
	defer decr()

	for i := 0; i < len(mts); i++ {
		val, ok := mts[i].sl.Search(key)
		if ok {
			return val, nil
		}
	}
	return db.lc.get(key)
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) write(entries []*Entry) error {
	var size int64
	for _, e := range entries {
		size += e.calculateSize()
	}
	if size >= db.opt.maxRequestSize {
		return ErrTooBig
	}
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	db.writeCh <- req
	return nil
}
