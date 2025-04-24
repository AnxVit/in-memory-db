package main

import (
	"GoKeyValueWarehouse/sstable"
	"GoKeyValueWarehouse/wal"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type DB struct {
	memtable     *memtable
	memtableLock *sync.RWMutex // ?

	sstables     []*sstable.SSTable
	sstablesLock *sync.RWMutex // ?

	flushQueue     []*memtable
	flushQueueLock *sync.Mutex
	flushQueueChan chan *memtable

	transactionsLock *sync.RWMutex
	transactions     []*Tranzaction

	isClosed atomic.Uint32

	wg   *sync.WaitGroup
	lock *sync.RWMutex

	opt       Options
	closeOnce sync.Once
	exit      chan struct{}
	compress  bool
}

func Open(opts Options) (*DB, error) {
	err := os.MkdirAll(opts.Directory, 0755)
	if err != nil {
		return nil, err
	}

	db := &DB{
		memtableLock: &sync.RWMutex{},

		sstablesLock: &sync.RWMutex{},

		flushQueue:     make([]*memtable, 0),
		flushQueueLock: &sync.Mutex{},

		transactionsLock: &sync.RWMutex{},
		transactions:     make([]*Tranzaction, 0),

		wg:   &sync.WaitGroup{},
		opt:  opts,
		exit: make(chan struct{}),
	}

	db.memtable, err = db.newMemtable()
	if err != nil {
		return nil, err
	}

	db.loadSSTables()

	// open the write ahead log
	wal, err := wal.Open(&db.opt.WALOpt)
	if err != nil {
		return nil, err
	}

	db.wg.Add(1)
	go db.backgroundWalWriter() // start the background wal writer
	// db.printLog("Background WAL writer started")

	db.wg.Add(1)
	go db.backgroundFlusher() // start the background flusher
	// db.printLog("Background flusher started")

	// Start the background compactor
	db.wg.Add(1)
	go db.backgroundCompactor() // start the background compactor
	// db.printLog("Background compactor started")

	//db.printLog("DB opened successfully")

	return db, nil
}

// TODO: change wg wait
func (db *DB) close() error {
	// db.printLog("Closing up")
	db.isClosed.Store(1)

	if db.memtable.sl.Size() > 0 {
		// db.printLog(fmt.Sprintf("Memtable is of size %d bytes and is being flushed to disk", db.memtable.Size()))
		db.appendMemtableToFlushQueue()
	}

	// signal the background operations to exit
	close(db.exit)

	// db.printLog("Waiting for background operations to finish")
	// wait for the background operations to finish
	db.wg.Wait()

	// db.printLog("Background operations finished")
	// db.printLog("Closing SSTables")

	for _, sstable := range db.sstables {
		err := sstable.Close()
		if err != nil {
			return err
		}
	}
	// db.printLog("SSTables closed")
	if db.memtable.wal != nil {
		// db.printLog("Closing WAL")
		err := db.memtable.wal.Close()
		if err != nil {
			return err
		}
		// db.printLog("WAL closed")
	}

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
		db.memtable, err = db.newMemtable()
		if err != nil {
			return errors.WithMessage(err, "cannot create new mem table")
		}

		return nil
	default:
		return errors.New("cannot drop to flushQueue")
	}
}

func (db *DB) flushMemtable(lc *sync.WaitGroup) {
	defer lc.Done()

	for mt := range db.flushQueueChan {
		if mt == nil {
			continue
		}

		for {
			if err := db.flushMemtableToDisk(mt); err != nil {
				time.Sleep(time.Second)
				continue
			}

			// Update s.imm. Need a lock.
			db.lock.Lock()

			db.flushQueue = db.flushQueue[1:]
			mt.DecrRef() // Return memory.
			// unlock
			db.lock.Unlock()
			break
		}
	}
}

func (db *DB) flushMemtableToDisk(mt *memtable) error {

}

func (db *DB) getMemTables() ([]*memtable, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*memtable
	tables = append(tables, db.memtable)
	db.memtable.IncrRef()

	// Get immutable memtables.
	last := len(db.flushQueue) - 1
	for i := range db.flushQueue {
		tables = append(tables, db.flushQueue[last-i])
		db.flushQueue[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (db *DB) get(key []byte) (interface{}, error) {
	if db.IsClosed() {
		return nil, ErrorClosedDB
	}
	tables, decr := db.getMemTables()
	defer decr()

	for i := 0; i < len(tables); i++ {
		val, ok := tables[i].sl.Search(key)
		if ok {
			return val, nil
		}
	}
	return db.lc.get(key, 0)
}
