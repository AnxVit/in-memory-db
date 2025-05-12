package db

import (
	"context"
	"sync"
)

type Tranzaction struct {
	db        *DB
	id        int64 // Unique identifier for the transaction
	entries   []*Entry
	operation []OPR_CODE
	lock      *sync.RWMutex
}

func (db *DB) Begin(ctx context.Context) *Tranzaction {
	db.transactionsLock.Lock()
	defer db.transactionsLock.Unlock()
	tx := &Tranzaction{
		db:        db,
		id:        int64(len(db.transactions)) + 1,
		entries:   make([]*Entry, 0),
		operation: make([]OPR_CODE, 0),
		lock:      &sync.RWMutex{},
	}
	db.transactions = append(db.transactions, tx)
	return tx
}

func (txn *Tranzaction) AddOperation(op OPR_CODE, key, value []byte) {
	txn.lock.Lock()
	defer txn.lock.Unlock()

	if op == READ {
		return
	}

	entry := &Entry{
		Key: key,
	}

	switch op {
	case INSERT, UPDATE:
		entry.Value = value
	case DELETE:
		entry.Value = TOMBSTONE
	}

	txn.entries = append(txn.entries, entry)
	txn.operation = append(txn.operation, op)
}

func (txn *Tranzaction) Commit() error {
	txn.db.memtableLock.Lock()
	defer txn.db.memtableLock.Unlock()

	txn.lock.Lock()
	defer txn.lock.Unlock()

	if err := txn.db.write(txn.entries); err != nil {
		return err
	}

	return nil
}
