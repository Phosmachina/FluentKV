package driver

import (
	"errors"
	"fmt"
	. "github.com/Phosmachina/FluentKV/core"
	"github.com/dgraph-io/badger"
	"os"
	"runtime"
	"sync/atomic"
)

const (
	defaultFileMode = 0755
)

type BadgerDB struct {
	Service *badger.DB
	closed  uint32 // if 1 is closed.
}

func NewBadgerDB(directoryPath string) (*KVStoreManager, error) {

	if directoryPath == "" {
		return nil, errors.New("directoryPath is empty")
	}

	lindex := directoryPath[len(directoryPath)-1]
	if lindex != os.PathSeparator && lindex != '/' {
		directoryPath += string(os.PathSeparator)
	}
	// create directories if necessary
	if err := os.MkdirAll(directoryPath, os.FileMode(defaultFileMode)); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(directoryPath).WithTruncate(true)

	service, err := badger.Open(opts)
	if err != nil {
		fmt.Printf("unable to initialize the badger-based session database: %v", err)
		return nil, err
	}

	db := &BadgerDB{Service: service}
	runtime.SetFinalizer(db, closeBadgerDB)

	return NewKVStoreManager(db), nil
}

func (db *BadgerDB) Close() {
	_ = closeBadgerDB(db)
}

func closeBadgerDB(db *BadgerDB) error {

	if atomic.LoadUint32(&db.closed) > 0 {
		return nil
	}
	err := db.Service.Close()
	if err == nil {
		atomic.StoreUint32(&db.closed, 1)
	}

	return err
}

var iterOptionsNoValues = badger.IteratorOptions{
	PrefetchValues: false,
	PrefetchSize:   100,
	Reverse:        false,
	AllVersions:    false,
}

// region KVDriver implementation

func (db *BadgerDB) RawSet(key IKey, value []byte) bool {

	err := db.Service.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(key.RawKey(), value))
	})

	return err == nil
}

func (db *BadgerDB) RawGet(key IKey) ([]byte, bool) {

	var value []byte

	err := db.Service.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key.RawKey())
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			value = v
			return nil
		})
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false
	}

	if err != nil {
		return nil, false
	}

	return value, true
}

func (db *BadgerDB) RawDelete(key IKey) bool {

	if !db.Exist(key) {
		return false
	}

	txn := db.Service.NewTransaction(true)
	err := txn.Delete(key.RawKey())

	if err != nil {
		return false
	}

	return txn.Commit() == nil
}

func (db *BadgerDB) RawIterKey(
	key IKey,
	action func(key IKey) (stop bool),
) {
	txn := db.Service.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(iterOptionsNoValues)
	defer iter.Close()

	for iter.Seek(key.RawPrefix()); iter.ValidForPrefix(key.RawPrefix()); iter.Next() {
		if action(NewKeyFromString(string(iter.Item().Key()))) {
			return
		}
	}
}

func (db *BadgerDB) RawIterKV(
	key IKey,
	action func(key IKey, value []byte) (stop bool),
) {
	txn := db.Service.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	var value []byte
	for iter.Seek(key.RawPrefix()); iter.ValidForPrefix(key.RawPrefix()); iter.Next() {
		valueCopy, _ := iter.Item().ValueCopy(value)

		k := string(iter.Item().Key())
		keyFromString := NewKeyFromString(k)
		if action(keyFromString, valueCopy) {
			return
		}
	}
}

func (db *BadgerDB) Exist(key IKey) bool {
	return db.Service.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key.RawKey())
		return err
	}) == nil
}

// endregion
