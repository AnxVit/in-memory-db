package main

import (
	"context"
	"fmt"
	"sync"
)

type Tranzaction struct {
	db        *DB
	id        int64    // Unique identifier for the transaction
	entries   []*Entry // List of operations in the transaction
	operation []OPR_CODE
	lock      *sync.RWMutex // The lock for the transaction
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

	entry := Entry{
		Key: key,
	}

	switch op {
	case INSERT, UPDATE:
		entry.Value = value
	case DELETE:
		entry.Value = TOMBSTONE
	}

	txn.entries = append(txn.entries)
	txn.operation = append(txn.operation, op)
}

func (txn *Tranzaction) Commit() error {
	txn.db.memtableLock.Lock() // Makes the transaction atomic and serializable
	defer txn.db.memtableLock.Unlock()

	txn.lock.Lock()
	defer txn.lock.Unlock()

	for _, entry := range txn.entries {
		txn.db.write(entry)
	}

	return nil
}

func (txn *Tranzaction) Rollback() error {
	txn.lock.Lock()
	defer txn.lock.Unlock()

	txn.db.memtableLock.Lock()
	defer txn.db.memtableLock.Unlock()

	for i := len(txn.entries) - 1; i >= 0; i-- {

		op := txn.operation[i]
		switch op {
		case INSERT, UPDATE:
			txn.db.write(&Entry{
				Key:   txn.entries[i].Key,
				Value: TOMBSTONE,
			})
		case DELETE:
			// err := txn.db.appendToWALQueue(PUT, op.Key, nil)
			// if err != nil {
			// 	return err
			// }
			// txn.db.memtable.Insert(op.Key, op.Value, nil)
		default:
			return fmt.Errorf("invalid operation")
		}
	}

	return nil
}
