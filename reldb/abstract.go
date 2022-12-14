package reldb

import (
	"errors"
	"strconv"
)

// AbstractRelDB pre-implement IRelationalDB,
// you need to set value of interface: methods implemented are used in abstract.
type AbstractRelDB struct{ IRelationalDB }

func (db *AbstractRelDB) GetNextKey() string {
	var key string
	if db.Count(PreAutoKavlb) == 0 { // Check tank have available keys for this table.
		cur := db.Count(PreAutoKused)
		for i := cur; i < cur+AutoKeyBuffer; i++ {
			db.RawSet(PreAutoKavlb, strconv.Itoa(i), nil)
		}
	}
	db.RawIterKey(MakePrefix(PreAutoKavlb), func(k string) (stop bool) { // Get next key.
		key = k
		_ = db.Delete(PreAutoKavlb, k)
		return true
	})
	return key
}

func (db *AbstractRelDB) FreeKey(keys ...string) []error {
	var errs []error
	for _, key := range keys {
		if !db.RawDelete(PreAutoKused, key) {
			errs = append(errs, errors.New("invalid id for FreeKey: "+key))
		} else {
			db.RawSet(PreAutoKavlb, key, nil)
		}
	}
	return errs
}

func (db *AbstractRelDB) CleanUnusedKey() {
	//TODO implement me
	panic("implement me")
}

func (db *AbstractRelDB) Insert(object *IObject) *ObjWrapper {
	objWrapper := NewObjWrapper(db, db.GetNextKey(), object)
	db.RawSet((*object).TableName(), objWrapper.ID, Encode(object))
	return objWrapper
}

func (db *AbstractRelDB) Set(id string, object *IObject) *ObjWrapper {
	db.RawSet((*object).TableName(), id, Encode(object))
	return NewObjWrapper(db, id, object)
}

func (db *AbstractRelDB) Get(tableName string, id string) *ObjWrapper {
	value, found := db.RawGet(tableName, id)
	if found {
		return NewObjWrapper(db, id, Decode(value))
	} else {
		return nil
	}
}

func (db *AbstractRelDB) Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper {
	objWrapper := db.Get(tableName, id)
	if objWrapper == nil {
		return nil
	}
	editor(objWrapper)
	db.Set(id, objWrapper.Value)
	return objWrapper
}

func (db *AbstractRelDB) Delete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (db *AbstractRelDB) DeepDelete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (db *AbstractRelDB) Count(tableName string) int {
	var ct = 0
	db.RawIterKey(MakePrefix(tableName), func(key string) (stop bool) {
		ct++
		return false
	})
	return ct
}

func (db *AbstractRelDB) Foreach(tableName string, do func(objWrapper ObjWrapper)) {
	db.RawIterKV(MakePrefix(tableName), func(key string, value []byte) (stop bool) {
		do(*NewObjWrapper(db, key, Decode(value)))
		return false
	})
}

func (db *AbstractRelDB) FindFirst(tableName string, predicate func(objWrapper ObjWrapper) bool) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (db *AbstractRelDB) FindAll(tableName string, predicate func(objWrapper ObjWrapper) bool) []*ObjWrapper {
	//TODO implement me
	panic("implement me")
}
