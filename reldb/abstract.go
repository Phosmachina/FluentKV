package fluentkv

import (
	"strconv"
	"strings"
	"sync"
)

// AbstractRelDB pre-implement IRelationalDB,
// you need to set the value of interface: methods implemented are used in abstract.
type AbstractRelDB struct {
	IRelationalDB
	availableKeys []string
	usedKeys      []string
	triggers      []ITrigger
	m             sync.Mutex
}

// NewAbstractRelDB initialize internal IRelationalDB and read db to determine current
// available and used keys.
func NewAbstractRelDB(db IRelationalDB) *AbstractRelDB {
	relDB := AbstractRelDB{}
	relDB.IRelationalDB = db

	maxVal := 0
	db.RawIterKey(PrefixTable, func(key string) (stop bool) {
		id := strings.Split(key, Delimiter)[1]
		relDB.usedKeys = append(relDB.usedKeys, id)
		val, _ := strconv.Atoi(id)
		if maxVal < val {
			maxVal = val
		}
		return false
	})
	for i := 0; i < AutoKeyBuffer*(1+maxVal/AutoKeyBuffer); i++ {
		key := strconv.Itoa(i)
		if IndexOf(key, relDB.usedKeys) == -1 {
			relDB.availableKeys = append(relDB.availableKeys, key)
		}
	}

	return &relDB
}

func (db *AbstractRelDB) getAbstractRelDB() *AbstractRelDB { return db }

func (db *AbstractRelDB) GetKey() string {

	db.m.Lock()

	var key string

	if db.availableKeys == nil {
		db.availableKeys = []string{}
		db.RawIterKey(PreAutoKavlb, func(k string) (stop bool) { // Get the next key.
			db.availableKeys = append(db.availableKeys, k)
			return false
		})
	}

	if len(db.usedKeys) == 0 {
		db.RawIterKey(PreAutoKused, func(key string) (stop bool) {
			db.usedKeys = append(db.usedKeys, key)
			return false
		})
	}

	if len(db.availableKeys) == 0 {
		for i := len(db.usedKeys); i < len(db.usedKeys)+AutoKeyBuffer; i++ {
			db.availableKeys = append(db.availableKeys, strconv.Itoa(i))
		}
	}

	key = db.availableKeys[0]
	db.usedKeys = append(db.usedKeys, key)
	db.availableKeys = db.availableKeys[1:]

	db.m.Unlock()

	return key
}

func (db *AbstractRelDB) FreeKey(keys ...string) {

	db.m.Lock()

	for _, key := range keys {
		if index := IndexOf(key, db.usedKeys); index != -1 {
			// Remove key from usedKeys
			db.usedKeys = append(db.usedKeys[:index], db.usedKeys[index+1:]...)

			// Add key to availableKeys
			db.availableKeys = append(db.availableKeys, key)
		}
	}

	db.m.Unlock()
}

func (db *AbstractRelDB) Insert(object IObject) string {

	id := db.GetKey()
	db.runTriggers(true, InsertOperation, id, object)
	db.RawSet(MakePrefix(object.TableName()), id, Encode(&object))
	db.runTriggers(false, InsertOperation, id, object)

	return id
}

func (db *AbstractRelDB) Set(id string, object IObject) error {

	if !db.Exist(object.TableName(), id) {
		return ErrInvalidId
	}
	db.runTriggers(true, UpdateOperation, id, object)
	db.RawSet(MakePrefix(object.TableName()), id, Encode(&object))
	db.runTriggers(false, UpdateOperation, id, object)

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
	db.runTriggers(true, UpdateOperation, id, value)
	if value == nil {
		return nil
	}
	edited := editor(*value)
	_ = db.Set(id, edited)
	db.runTriggers(false, UpdateOperation, id, edited)

	return &edited
}

func (db *AbstractRelDB) Delete(tableName string, id string) error {

	// TODO add call to run trigger
	db.runTriggers(true, DeleteOperation, id, nil)
	if !db.RawDelete(MakePrefix(tableName), id) {
		return ErrInvalidId
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
	// TODO add call to run trigger
	db.runTriggers(false, DeleteOperation, id, nil)

	return nil
}

func (db *AbstractRelDB) DeepDelete(tableName string, id string) error {

	if !db.RawDelete(MakePrefix(tableName), id) {
		return ErrInvalidId
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

	pool := NewTaskPool()

	db.RawIterKV(MakePrefix(tableName), func(key string, value []byte) (stop bool) {
		valCp := value
		keyCp := key
		pool.AddTask(
			func() {
				decode := Decode(valCp)
				do(keyCp, decode)
			})
		return false
	})

	pool.Close()
}

func (db *AbstractRelDB) FindFirst(tableName string, predicate func(id string, value *IObject) bool) (string, *IObject) {

	var resultId = ""
	var resultValue *IObject
	// TODO parallelized inner operation

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

	pool := NewTaskPool()

	db.RawIterKV(MakePrefix(tableName), func(key string, value []byte) (stop bool) {
		valCp := value
		keyCp := key
		pool.AddTask(func() {
			curObj := Decode(valCp)
			if predicate(keyCp, curObj) {
				resultIds = append(resultIds, keyCp)
				resultValues = append(resultValues, curObj)
			}
		})
		return false
	})

	pool.Close()

	return resultIds, resultValues
}

// region Fluent toolkit

// TableName is a helper to call the TableName from T IObject implementation.
func TableName[T IObject]() string {
	var t T
	return t.TableName()
}

// Insert in the db the value and return the resulting wrapper.
func Insert[T IObject](db IRelationalDB, value T) *ObjWrapper[T] {
	id := db.Insert(value)
	return NewObjWrapper(db, id, &value)
}

// Set override the value at the id specified with the passed value. The id shall exist.
func Set[T IObject](db IRelationalDB, id string, value T) *ObjWrapper[T] {

	if err := db.Set(id, value); err != nil {
		return nil
	}

	return NewObjWrapper(db, id, &value)
}

// SetWrp same as set but take a wrapped object in argument.
func SetWrp[T IObject](db IRelationalDB, objWrp ObjWrapper[T]) *ObjWrapper[T] {
	return Set(db, objWrp.ID, objWrp.Value)
}

// Get the value in db based on id and the tableName induced by the T parameter.
func Get[T IObject](db IRelationalDB, id string) *ObjWrapper[T] {

	get := db.Get(TableName[T](), id)

	if get == nil {
		return nil
	}
	t := (*get).(T)

	return NewObjWrapper(db, id, &t)
}

// Update the value determine with the id and the tableName induced by the T parameter.
// The result of the editor function is set in the db.
func Update[T IObject](db IRelationalDB, id string, editor func(value *T)) *ObjWrapper[T] {

	var t T

	db.Update(TableName[T](), id, func(value IObject) IObject {
		t = (value).(T)
		editor(&t)
		return t
	})

	return NewObjWrapper(db, id, &t)
}

// Delete the object determine with the id and the tableName induced by the T parameter.
// id is released and related link are deleted.
func Delete[T IObject](db IRelationalDB, id string) error {
	return db.Delete(TableName[T](), id)
}

// DeleteWrp the object determine with the id and the tableName induced by the T parameter.
// id is released and related link are deleted.
func DeleteWrp[T IObject](objWrp *ObjWrapper[T]) error {
	return Delete[T](objWrp.db, objWrp.ID)
}

// DeepDelete the object determine with the id and the tableName induced by the T parameter and
// all object directly connected.
func DeepDelete[T IObject](db IRelationalDB, id string) error {
	return db.DeepDelete(TableName[T](), id)
}

// DeepDeleteWrp the object determine with the id and the tableName induced by the T parameter and
// all object directly connected.
func DeepDeleteWrp[T IObject](objWrp *ObjWrapper[T]) error {
	return DeepDelete[T](objWrp.db, objWrp.ID)
}

// Exist return true if the object determine with the id and the tableName induced by the T
// parameter exist in db.
func Exist[T IObject](db IRelationalDB, id string) bool {
	return db.Exist(TableName[T](), id)
}

// ExistWrp return true if the object determine with the id and the tableName induced by the T
// parameter exist in db.
func ExistWrp[T IObject](objWrp *ObjWrapper[T]) bool {
	return Exist[T](objWrp.db, objWrp.ID)
}

// Count return the count of object in the table based on the tableName induced by the T parameter.
func Count[T IObject](db IRelationalDB) int {
	return db.Count(MakePrefix(TableName[T]()))
}

// Foreach iterating on the table based on the tableName induced by the T parameter
// and execute the do function on each value.
func Foreach[T IObject](db IRelationalDB, do func(id string, value *T)) {
	db.Foreach(TableName[T](), func(id string, value *IObject) {
		t := (*value).(T)
		do(id, &t)
	})
}

// FindFirst iterate on the table based on the tableName induced by the T parameter and execute the
// predicate function on each value until it return true value: the current value is returned.
func FindFirst[T IObject](db IRelationalDB, predicate func(id string, value *T) bool) *ObjWrapper[T] {

	resultId, resultValue := db.FindFirst(TableName[T](), func(id string, value *IObject) bool {
		t := (*value).(T)
		return predicate(id, &t)
	})

	if resultValue == nil {
		return nil
	}
	t := (*resultValue).(T)

	return NewObjWrapper(db, resultId, &t)
}

// FindAll iterate on the table based on the tableName induced by the T parameter and execute the
// predicate function on each value. All values matching the predicate are returned.
func FindAll[T IObject](db IRelationalDB, predicate func(id string, value *T) bool) []*ObjWrapper[T] {

	var objs []*ObjWrapper[T]

	ids, results := db.FindAll(TableName[T](), func(id string, value *IObject) bool {
		t := (*value).(T)
		return predicate(id, &t)
	})

	for i, curId := range ids {
		t := (*results[i]).(T)
		objs = append(objs, NewObjWrapper(db, curId, &t))
	}

	return objs
}

// endregion

// region Trigger

// Operation represents CRUD operations in bitmask format.
type Operation int

// Contain checks if the Operation in parameter is present in this Operation.
func (o Operation) Contain(s Operation) bool {
	return o&s == s
}

// CRUD Operation used for trigger as filter.
const (
	GetOperation Operation = 1 << iota
	InsertOperation
	DeleteOperation
	UpdateOperation
)

type ITrigger interface {
	GetId() string
	GetTableName() string
	GetOperation() Operation
	IsBefore() bool
	Start(string, IObject)
	Equals(other ITrigger) bool
}

type trigger[T IObject] struct {
	id         string
	tableName  string
	operations Operation
	isBefore   bool
	action     func(id string, value T)
}

func (t trigger[T]) GetId() string {
	return t.id
}

func (t trigger[T]) GetTableName() string {
	return t.tableName
}
func (t trigger[T]) GetOperation() Operation {
	return t.operations
}

func (t trigger[T]) IsBefore() bool {
	return t.isBefore
}

func (t trigger[T]) Start(id string, value IObject) {
	t.action(id, value.(T))
}

func (t trigger[T]) Equals(other ITrigger) bool {
	return t.GetTableName() == other.GetTableName() && t.GetId() == other.GetId()
}

func (db *AbstractRelDB) runTriggers(
	isBefore bool,
	operation Operation,
	id string,
	value IObject,
) {
	pool := NewTaskPool()

	for _, triggerToBeRan := range db.triggers {

		if triggerToBeRan.IsBefore() == isBefore &&
			triggerToBeRan.GetTableName() == value.TableName() &&
			triggerToBeRan.GetOperation().Contain(operation) {

			idCp := id
			valCp := value
			pool.AddTask(func() {
				triggerToBeRan.Start(idCp, valCp)
			})
		}
	}

	pool.Close()
}

// AddTrigger register a new trigger with given parameter and table name inferred form
// the T parameter.
//
// Parameters:
//
// id: a string that will be used as the identifier of the new trigger: it could be a
// description but should be unique relatively to the table name.
//
// operations: a value of type Operation defining the operations that will trigger the
// action.
//
// isBefore: a boolean indicating if the trigger should be processed before
// (true) or after (false) the operation.
//
// action: a function that will be executed when the trigger fires with the id and
// value of the current operation.
//
// Returns ErrDuplicateTrigger if a trigger with the same id already exists in the
// provided database.
func AddTrigger[T IObject](
	idb IRelationalDB,
	id string,
	operations Operation,
	isBefore bool,
	action func(id string, value T),
) error {

	db := idb.getAbstractRelDB()

	triggerToBeAdded := trigger[T]{
		id:         id,
		tableName:  TableName[T](),
		operations: operations,
		isBefore:   isBefore,
		action:     action,
	}

	db.m.Lock()
	index := IndexOf[ITrigger](triggerToBeAdded, db.triggers)
	if index != -1 {
		return ErrDuplicateTrigger
	}

	db.triggers = append(db.triggers, triggerToBeAdded)
	db.m.Unlock()

	return nil
}

// DeleteTrigger deletes a trigger from registered triggers based on the table name
// inferred by the T parameter and the id.
//
// If the trigger is not present, it returns ErrInexistantTrigger.
func DeleteTrigger[T IObject](idb IRelationalDB, id string,
) error {

	db := idb.getAbstractRelDB()

	triggerToBeAdded := trigger[T]{
		id:        id,
		tableName: TableName[T](),
	}

	db.m.Lock()
	index := IndexOf[ITrigger](triggerToBeAdded, db.triggers)
	if index == -1 {
		return ErrInexistantTrigger
	}

	db.triggers = append(db.triggers[index:], db.triggers[:index+1]...)
	db.m.Unlock()

	return nil
}

// endregion
