package main

import (
	"GoKeyValueWarehouse/operation"
	"context"
	"fmt"
	"sync"
)

type Tranzaction struct {
	db   *DB
	id   int64                  // Unique identifier for the transaction
	ops  []*operation.Operation // List of operations in the transaction
	lock *sync.RWMutex          // The lock for the transaction
}

func (db *DB) Begin(ctx context.Context) *Tranzaction {
	db.transactionsLock.Lock()
	defer db.transactionsLock.Unlock()
	tx := &Tranzaction{
		db:   db,
		id:   int64(len(db.transactions)) + 1,
		ops:  make([]*operation.Operation, 0),
		lock: &sync.RWMutex{},
	}
	db.transactions = append(db.transactions, tx)
	return tx
}

func (txn *Tranzaction) AddOperation(op operation.OPR_CODE, key, value []byte) {

	txn.lock.Lock()
	defer txn.lock.Unlock()

	if op == operation.READ {
		return
	}

	oper := &operation.Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	switch op {
	case operation.INSERT:
		// On PUT operation, the rollback operation is DELETE
		oper.Rollback = &operation.Operation{
			Op:    operation.DELETE,
			Key:   key,
			Value: nil,
		}
	case operation.DELETE:
		// On DELETE operation we can put back the key value pair
		oper.Rollback = &Operation{
			Op:    operation.INSERT,
			Key:   key,
			Value: value,
		}
	case operation.READ:
		oper.Rollback = nil // GET operations are read-only
	}

	txn.ops = append(txn.ops, oper)
}

func (txn *Tranzaction) Commit() error {
	txn.db.memtableLock.Lock() // Makes the transaction atomic and serializable
	defer txn.db.memtableLock.Unlock()

	// Lock the transaction
	txn.lock.Lock()
	defer txn.lock.Unlock()

	// Apply operations to memtable
	for _, op := range txn.ops {
		switch op.Op {
		case PUT:
			// Append operation to WAL queue
			err := txn.db.appendToWALQueue(PUT, op.Key, op.Value)
			if err != nil {
				return err
			}

			txn.db.memtable.Insert(op.Key, op.Value, nil) // we don't use put, we use insert
		case DELETE:
			err := txn.db.appendToWALQueue(DELETE, op.Key, nil)
			if err != nil {
				return err
			}

			txn.db.memtable.Insert(op.Key, []byte(TOMBSTONE_VALUE), nil)
		// GET operations are read-only

		default:
			// Rollback transaction
			err := txn.Rollback()
			if err != nil {
				return err
			}
			return fmt.Errorf("invalid operation")
		}
	}

	// Check if memtable needs to be flushed
	if txn.db.memtable.Size() >= txn.db.memtableFlushThreshold {
		txn.db.appendMemtableToFlushQueue() // Append memtable to flush queue
	}

	return nil
}

func (txn *Tranzaction) Rollback() error {
	txn.lock.Lock()
	defer txn.lock.Unlock()

	txn.db.memtableLock.Lock()
	defer txn.db.memtableLock.Unlock()

	for i := len(txn.ops) - 1; i >= 0; i-- {

		op := txn.ops[i]
		switch op.Op {
		case PUT:
			err := txn.db.appendToWALQueue(PUT, op.Key, []byte(TOMBSTONE))
			if err != nil {
				return err
			}
			txn.db.memtable.Insert(op.Key, []byte(TOMBSTONE), nil)
		case DELETE:
			err := txn.db.appendToWALQueue(PUT, op.Key, nil)
			if err != nil {
				return err
			}
			txn.db.memtable.Insert(op.Key, op.Value, nil)
		default:
			return fmt.Errorf("invalid operation")
		}
	}

	return nil
}

func (txn *Tranzaction) Remove() {
	txn.ops = make([]*operation.Operation, 0)

	txn.db.transactionsLock.Lock()
	for i, t := range txn.db.txn {
		if t == txn {
			txn.db.txn = append(txn.db.txn[:i], txn.db.txn[i+1:]...)
			break
		}
	}

	txn.db.transactionsLock.Unlock()
}
