package reldb

import (
	"errors"
	"strconv"
	"strings"
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
	db.RawIterKey(PreAutoKavlb, func(k string) (stop bool) { // Get next key.
		key = k
		_ = db.RawDelete(PreAutoKavlb, k)
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

func (db *AbstractRelDB) Insert(object IObject) string {
	id := db.GetNextKey()
	db.RawSet(MakePrefix(object.TableName()), id, Encode(&object))
	return id
}

func (db *AbstractRelDB) Set(id string, object IObject) error {
	if !db.Exist(object.TableName(), id) {
		return InvalidId
	}
	db.RawSet(MakePrefix(object.TableName()), id, Encode(&object))
	return nil
}

func (db *AbstractRelDB) SetWrp(objWrp ObjWrapper[IObject]) error {
	return db.Set(objWrp.ID, objWrp.Value)
}

func (db *AbstractRelDB) Get(tableName string, id string) *IObject {
	value, found := db.RawGet(MakePrefix(tableName), id)
	if found {
		return Decode(value)
	} else {
		return nil
	}
}

func (db *AbstractRelDB) Update(tableName string, id string, editor func(value IObject) IObject) *IObject {
	value := db.Get(tableName, id)
	if value == nil {
		return nil
	}
	edited := editor(*value)
	_ = db.Set(id, edited)
	return &edited
}

func (db *AbstractRelDB) Delete(tableName string, id string) error {
	if !db.RawDelete(MakePrefix(tableName), id) {
		return InvalidId
	}
	db.FreeKey(id)
	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		for _, s := range strings.Split(key, LinkDelimiter) {
			tnAndId := strings.Split(s, Delimiter)
			if tnAndId[0] == tableName && tnAndId[1] == id {
				db.RawDelete(PrefixLink, key)
			}
		}
		return false
	})
	return nil
}

func (db *AbstractRelDB) DeepDelete(tableName string, id string) error {
	if !db.RawDelete(MakePrefix(tableName), id) {
		return InvalidId
	}
	db.FreeKey(id)
	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		split := strings.Split(key, LinkDelimiter)
		tnAndIdK1 := strings.Split(split[0], Delimiter)
		tnAndIdK2 := strings.Split(split[1], Delimiter)
		if tnAndIdK1[0] == tableName && tnAndIdK1[1] == id {
			db.RawDelete(PrefixLink, key)
			_ = db.DeepDelete(tnAndIdK2[0], tnAndIdK2[1])
		} else if tnAndIdK2[0] == tableName && tnAndIdK2[1] == id {
			db.RawDelete(PrefixLink, key)
		}
		return false
	})
	return nil
}

func (db *AbstractRelDB) Count(prefix string) int {
	var ct = 0
	db.RawIterKey(prefix, func(key string) (stop bool) {
		ct++
		return false
	})
	return ct
}

func (db *AbstractRelDB) Foreach(tableName string, do func(id string, value *IObject)) {
	db.RawIterKV(MakePrefix(tableName), func(key string, value []byte) (stop bool) {
		do(key, Decode(value))
		return false
	})
}

func (db *AbstractRelDB) FindFirst(tableName string, predicate func(id string, value *IObject) bool) (string, *IObject) {
	var resultId = ""
	var resultValue *IObject
	db.RawIterKV(MakePrefix(tableName), func(curId string, value []byte) (stop bool) {
		tmpObj := Decode(value)
		if predicate(curId, tmpObj) {
			resultId = curId
			resultValue = tmpObj
			return true
		}
		return false
	})
	return resultId, resultValue
}

func (db *AbstractRelDB) FindAll(tableName string, predicate func(id string, value *IObject) bool) ([]string, []*IObject) {
	var resultIds []string
	var resultValues []*IObject
	db.RawIterKV(MakePrefix(tableName), func(key string, value []byte) (stop bool) {
		curObj := Decode(value)
		if predicate(key, curObj) {
			resultIds = append(resultIds, key)
			resultValues = append(resultValues, curObj)
		}
		return false
	})
	return resultIds, resultValues
}

//region Fluent toolkit

func tableName[T IObject]() string {
	var t T
	return t.TableName()
}

func Insert[T IObject](db IRelationalDB, value T) *ObjWrapper[T] {
	id := db.Insert(value)
	return NewObjWrapper(db, id, &value)
}

func Set[T IObject](db IRelationalDB, id string, value T) *ObjWrapper[T] {
	err := db.Set(id, value)
	if err != nil {
		return nil
	}
	return NewObjWrapper(db, id, &value)
}

func SetWrp[T IObject](db IRelationalDB, objWrp ObjWrapper[T]) *ObjWrapper[T] {
	return Set(db, objWrp.ID, objWrp.Value)
}

func Get[T IObject](db IRelationalDB, id string) *ObjWrapper[T] {
	get := db.Get(tableName[T](), id)
	if get == nil {
		return nil
	}
	t := (*get).(T)
	return NewObjWrapper(db, id, &t)
}

func Update[T IObject](db IRelationalDB, id string, editor func(value *T)) *ObjWrapper[T] {
	var t T
	db.Update(tableName[T](), id, func(value IObject) IObject {
		t = (value).(T)
		editor(&t)
		return t
	})
	return NewObjWrapper(db, id, &t)
}

func Delete[T IObject](db IRelationalDB, id string) error {
	return db.Delete(tableName[T](), id)
}

func DeepDelete[T IObject](db IRelationalDB, id string) error {
	return db.DeepDelete(tableName[T](), id)
}

func Exist[T IObject](db IRelationalDB, id string) bool {
	return db.Exist(tableName[T](), id)
}

func Count[T IObject](db IRelationalDB) int {
	return db.Count(MakePrefix(tableName[T]()))
}

func Foreach[T IObject](db IRelationalDB, do func(id string, value *T)) {
	db.Foreach(tableName[T](), func(id string, value *IObject) {
		t := (*value).(T)
		do(id, &t)
	})
}

func FindFirst[T IObject](db IRelationalDB, predicate func(id string, value *T) bool) *ObjWrapper[T] {
	resultId, resultValue := db.FindFirst(tableName[T](), func(id string, value *IObject) bool {
		t := (*value).(T)
		return predicate(id, &t)
	})
	if resultValue == nil {
		return nil
	}
	t := (*resultValue).(T)
	return NewObjWrapper(db, resultId, &t)
}

func FindAll[T IObject](db IRelationalDB, predicate func(id string, value *T) bool) []*ObjWrapper[T] {
	var objs []*ObjWrapper[T]
	ids, results := db.FindAll(tableName[T](), func(id string, value *IObject) bool {
		t := (*value).(T)
		return predicate(id, &t)
	})
	for i, curId := range ids {
		t := (*results[i]).(T)
		objs = append(objs, NewObjWrapper(db, curId, &t))
	}
	return objs
}

//endregion
