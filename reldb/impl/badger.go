package driver

import (
	"bytes"
	"errors"
	. "git.antlia.tk/naxxar/FluentKV/reldb"
	"github.com/dgraph-io/badger"
	"github.com/kataras/golog"
	"os"
	"runtime"
	"sync/atomic"
)

const (
	defaultFileMode = 0755
)

type BadgerDB struct {
	AbstractRelDB
	Service *badger.DB
	closed  uint32 // if 1 is closed.
}

func NewBadgerDB(directoryPath string) (IRelationalDB, error) {
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
		golog.Errorf("unable to initialize the badger-based session database: %v", err)
		return nil, err
	}

	db := &BadgerDB{Service: service}
	db.IRelationalDB = db
	runtime.SetFinalizer(db, closeDB)

	return db, nil
}

func closeDB(db *BadgerDB) error {

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

// region IRelationalDB implementation

func (db *BadgerDB) RawSet(prefix string, key string, value []byte) {

	err := db.Service.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry([]byte(prefix+key), value))
	})

	if err != nil {
		golog.Error(err)
	}
}

func (db *BadgerDB) RawGet(prefix string, key string) ([]byte, bool) {

	var value []byte

	err := db.Service.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + key))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			value = v
			return nil
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, false
	}

	if err != nil {
		golog.Error(err)
		return nil, false
	}

	return value, true
}

func (db *BadgerDB) RawDelete(prefix string, key string) bool {

	txn := db.Service.NewTransaction(true)
	err := txn.Delete([]byte(prefix + key))

	if err != nil {
		golog.Error(err)
		return false
	}

	return txn.Commit() == nil
}

func (db *BadgerDB) RawIterKey(prefix string, action func(key string) (stop bool)) {

	txn := db.Service.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(iterOptionsNoValues)
	defer iter.Close()

	pfx := []byte(prefix)
	for iter.Seek(pfx); iter.ValidForPrefix(pfx); iter.Next() {
		if action(string(bytes.TrimPrefix(iter.Item().Key(), pfx))) {
			return
		}
	}
}

func (db *BadgerDB) RawIterKV(prefix string, action func(key string, value []byte) (stop bool)) {

	txn := db.Service.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	pfx := []byte(prefix)
	for iter.Seek(pfx); iter.ValidForPrefix(pfx); iter.Next() {
		var value []byte
		_ = iter.Item().Value(func(valueBytes []byte) error {
			value = valueBytes
			return nil
		})

		if action(string(bytes.TrimPrefix(iter.Item().Key(), pfx)), value) {
			return
		}
	}
}

func (db *BadgerDB) Exist(tableName string, id string) bool {
	return db.Service.View(func(txn *badger.Txn) error {
		_, err := txn.Get(MakeKey(tableName, id))
		return err
	}) == nil
}

// endregion
