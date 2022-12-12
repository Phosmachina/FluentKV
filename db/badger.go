package db

import (
	. "Relational_Badger/reldb"
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/kataras/golog"
	"os"
	. "relational-badger/reldb"
	"runtime"
	"strconv"
	"sync/atomic"
)

const (
	defaultFileMode = 0755

	autoKeyBuffer = 50
	prefixAutoKey = "tank%"

	PrefixLink  = "link%"
	PrefixTable = "tbl%"
)

type BadgerDB struct {
	Service *badger.DB
	closed  uint32 // if 1 is closed.
}

func NewBadgerDB(directoryPath string) (RelDB, error) {
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

//region Helper

func makeKey(sid, key string) []byte {
	return append([]byte(sid), []byte(key)...)
}

// Find loops through all session keys with filter and values and return first value found.
func (db *BadgerDB) find(prefix string, f func(key string, value []byte) (found bool)) (bool,
	[]byte) {

	txn := db.Service.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	tn := []byte(prefix)
	for iter.Seek(tn); iter.ValidForPrefix(tn); iter.Next() {
		item := iter.Item()

		var value []byte
		_ = item.Value(func(valueBytes []byte) error {
			value = valueBytes
			return nil
		})

		if f(string(bytes.TrimPrefix(item.Key(), tn)), value) {
			return true, value
		}
	}

	return false, nil
}

//endregion

//region RelDB implementation

func (db *BadgerDB) RawSet(prefix string, key string, value []byte) {

	err := db.Service.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(makeKey(prefix, key), value))
	})

	if err != nil {
		golog.Error(err)
	}
}

func (db *BadgerDB) Insert(object *Object) *ObjWrapper {
	var buffer bytes.Buffer
	err := gob.NewEncoder(&buffer).Encode(object)
	if err != nil {
		golog.Error(err)
		return nil
	}

	objWrapper := NewObjWrapper(RelDB(db), object)
	tnk := prefixAutoKey + (*object).TableName()
	tnv := PrefixTable + (*object).TableName()

	if db.Count(tnk) == 0 { // Check tank have available keys for this table.
		cur := db.Count(tnv)
		for i := cur; i < cur+autoKeyBuffer; i++ {
			db.RawSet(tnk, strconv.Itoa(i), nil)
		}
	}
	db.find(tnk, func(k string, _ []byte) bool { // Get next key.
		objWrapper.ID = k
		_ = db.Delete(tnk, k)
		return true
	})

	err = db.Service.Update(func(txn *badger.Txn) error { // Create the record with key.
		return txn.SetEntry(badger.NewEntry(makeKey(tnv, objWrapper.ID), buffer.Bytes()))
	})

	if err != nil {
		golog.Error(err)
	}

	return nil
}

func (db *BadgerDB) Set(id string, object *Object) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Get(tableName string, id string) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Delete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) DeepDelete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Exist(tableName string, id string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Count(tableName string) int {

	var ct = 0
	txn := db.Service.NewTransaction(false)
	iter := txn.NewIterator(iterOptionsNoValues)

	prefix := append([]byte(PrefixTable), []byte(tableName)...)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		ct++
	}

	iter.Close()
	txn.Discard()
	return ct
}

func (db *BadgerDB) Foreach(tableName string, do func(objWrapper ObjWrapper)) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) FindFirst(tableName string, predicate func(objWrapper ObjWrapper) bool) (
	*ObjWrapper, error) {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) FindAll(tableName string, predicate func(objWrapper ObjWrapper) bool) (
	[]*ObjWrapper, error) {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerDB) Print(tableName string) error {
	//TODO implement me
	panic("implement me")
}

//endregion
