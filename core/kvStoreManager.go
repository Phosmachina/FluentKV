package core

import (
	"errors"
	. "github.com/Phosmachina/FluentKV/helper"
	"strconv"
	"sync"
)

var (
	AutoIdBuffer = 1000

	ErrInvalidId   = errors.New("the Id specified is nod used in the db")
	ErrFailedToSet = errors.New("the set operation failed")
	ErrSelfBind    = errors.New("try to link object to itself")
)

type KVStoreManager struct {
	KVDriver
	marshaller   IMarshaller
	availableIds []string
	usedIds      []string
	triggers     []ITrigger
	m            sync.Mutex // TODO combine its use with defer
}

// NewKVStoreManager initialize internal KVDriver and read db to determine current
// available and used keys.
func NewKVStoreManager(driver KVDriver) *KVStoreManager {

	kvStoreManager := KVStoreManager{
		KVDriver:   driver,
		marshaller: &GobMarshaller{},
	}

	biggestId := 0

	driver.RawIterKey(NewProtoTableKey(), func(key IKey) (stop bool) {
		id := key.(*TableKey).Id()
		kvStoreManager.usedIds = append(kvStoreManager.usedIds, id)

		idAsInt, _ := strconv.Atoi(id)
		if biggestId < idAsInt {
			biggestId = idAsInt
		}

		return false
	})

	for i := 0; i < AutoIdBuffer*(1+biggestId/AutoIdBuffer); i++ {
		key := strconv.Itoa(i)
		if IndexOf(key, kvStoreManager.usedIds) == -1 {
			kvStoreManager.availableIds = append(kvStoreManager.availableIds, key)
		}
	}

	return &kvStoreManager
}

func (db *KVStoreManager) SetMarshaller(marshaller IMarshaller) *KVStoreManager {
	db.marshaller = marshaller
	return db
}

func (db *KVStoreManager) Marshaller() IMarshaller {
	return db.marshaller
}

// GetFreeId pick a key in the tank of unique ids.
func (db *KVStoreManager) GetFreeId() string {

	db.m.Lock()

	if db.availableIds == nil {
		db.availableIds = []string{}
		db.RawIterKey(TankAvailableKey{}, func(key IKey) (stop bool) { // Get the next Id.
			db.availableIds = append(db.availableIds, key.(TankAvailableKey).Id())
			return false
		})
	}

	if len(db.usedIds) == 0 {
		db.RawIterKey(TankUsedKey{}, func(key IKey) (stop bool) {
			db.usedIds = append(db.usedIds, key.(TankUsedKey).Id())
			return false
		})
	}

	if len(db.availableIds) == 0 {
		for i := len(db.usedIds); i < len(db.usedIds)+AutoIdBuffer; i++ {
			db.availableIds = append(db.availableIds, strconv.Itoa(i))
		}
	}

	id := db.availableIds[0]
	db.usedIds = append(db.usedIds, id)
	db.availableIds = db.availableIds[1:]

	db.m.Unlock()

	return id
}

// FreeId check if the id is in use and make the key available again.
func (db *KVStoreManager) FreeId(ids ...string) {

	db.m.Lock()

	for _, id := range ids {
		if index := IndexOf(id, db.usedIds); index != -1 {
			// Remove the id from usedIds
			db.usedIds = append(db.usedIds[:index], db.usedIds[index+1:]...)

			// Add the id to availableIds
			db.availableIds = append(db.availableIds, id)
		}
	}

	db.m.Unlock()
}

func (db *KVStoreManager) Insert(value *any) (*TableKey, error) {

	tableKey := NewTableKeyFromObject(*value).SetId(db.GetFreeId())

	errs := db.withTriggerWrapper(tableKey, value, InsertOperation, func() error {
		encode, err := db.marshaller.Encode(value)
		if err != nil {
			return err
		}

		if !db.RawSet(tableKey, encode) {
			return ErrFailedToSet
		}
		return nil
	})

	if errs != nil {
		db.FreeId(tableKey.Id())
	}

	return tableKey, errs
}

func (db *KVStoreManager) Set(tableKey *TableKey, value *any) error {

	if !db.Exist(tableKey) {
		return ErrInvalidId
	}

	return db.withTriggerWrapper(tableKey, value, UpdateOperation, func() error {
		encode, err := db.marshaller.Encode(value)
		if err != nil {
			return err
		}
		if !db.RawSet(tableKey, encode) {
			return ErrFailedToSet
		}
		return nil
	})
}

func (db *KVStoreManager) Get(tableKey *TableKey) (*any, error) {

	var value *any

	rawValue, found := db.RawGet(tableKey)
	if found {
		var err error
		value, err = db.marshaller.Decode(rawValue)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrInvalidId
	}

	// TODO don't report trigger action error to an API call!
	err := db.withTriggerWrapper(tableKey, value, GetOperation, func() error {
		return nil
	})

	return value, err
}

func (db *KVStoreManager) Update(
	tableKey *TableKey,
	editor func(value *any) *any,
) (*any, error) {

	raw, found := db.RawGet(tableKey)
	if !found {
		return nil, ErrInvalidId
	}

	value, err := db.marshaller.Decode(raw)
	if err != nil {
		return nil, err
	}

	err = db.withTriggerWrapper(tableKey, value, UpdateOperation, func() error {
		*value = *editor(value)
		rawUpdatedValue, err2 := db.marshaller.Encode(value)
		if err2 != nil {
			return err2
		}
		if !db.RawSet(tableKey, rawUpdatedValue) {
			return ErrFailedToSet
		}

		return nil
	})

	return value, err
}

func (db *KVStoreManager) Delete(tableKey *TableKey) error {

	// Use raw operation to bypass triggers on Get operation.
	raw, found := db.RawGet(tableKey)
	if !found {
		return ErrInvalidId
	}
	value, err := db.marshaller.Decode(raw)
	if err != nil {
		return err
	}

	return db.withTriggerWrapper(tableKey, value, DeleteOperation, func() error {
		if !db.RawDelete(tableKey) {
			return ErrInvalidId
		}
		db.FreeId(tableKey.Id())

		// Delete all links that connect another value to the current value and vice versa.
		db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {

			linkKey := key.(*LinkKey)

			if linkKey.currentTableKey.Equals(tableKey) ||
				linkKey.targetTableKey.Equals(tableKey) {
				db.RawDelete(linkKey)
			}

			return false
		})

		return nil
	})
}

func (db *KVStoreManager) DeepDelete(tableKey *TableKey) error {

	// Use raw operation to bypass triggers on Get operation.
	raw, found := db.RawGet(tableKey)
	if !found {
		return ErrInvalidId
	}
	value, err := db.marshaller.Decode(raw)
	if err != nil {
		return err
	}

	return db.withTriggerWrapper(tableKey, value, DeleteOperation, func() error {

		if !db.RawDelete(tableKey) {
			return ErrInvalidId
		}

		db.FreeId(tableKey.Id())

		db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {

			linkKey := key.(*LinkKey)

			if linkKey.CurrentTableKey().Equals(tableKey) {
				// Delete the link and DeepDelete the connected value.
				db.RawDelete(linkKey)
				_ = db.DeepDelete(linkKey.TargetTableKey())
			} else if linkKey.TargetTableKey().Equals(tableKey) {
				// Delete the link that connects another value to the current value.
				db.RawDelete(linkKey)
			}

			return false
		})

		return nil
	})
}

func (db *KVStoreManager) Count(key *TableKey) int {

	var ct = 0
	db.RawIterKey(key, func(_ IKey) (stop bool) {
		ct++
		return false
	})

	return ct
}

func (db *KVStoreManager) Foreach(
	tableKey *TableKey,
	do func(tableKey *TableKey, value *any),
) {

	pool := NewTaskPool()

	db.RawIterKV(tableKey, func(key IKey, rawValue []byte) (stop bool) {
		valCopy := rawValue
		keyCopy := key.(*TableKey)
		pool.AddTask(
			func() {
				decode, err := db.marshaller.Decode(valCopy)
				if err != nil {
					return
				}
				do(keyCopy, decode)
			})

		return false
	})

	pool.Close()
}

func (db *KVStoreManager) FindFirst(
	tableKey *TableKey,
	predicate func(tableKey *TableKey, value *any) bool,
) (*TableKey, *any) {

	var resultKey *TableKey
	var resultValue *any
	// TODO parallelized inner operation

	db.RawIterKV(tableKey, func(key IKey, rawValue []byte) (stop bool) {
		tmpValue, err := db.marshaller.Decode(rawValue)
		if err != nil {
			return false
		}

		tmpKey := key.(*TableKey)
		if predicate(resultKey, tmpValue) {
			resultKey = tmpKey
			resultValue = tmpValue
			return true
		}

		return false
	})

	return resultKey, resultValue
}

func (db *KVStoreManager) FindAll(
	tableKey *TableKey,
	predicate func(tableKey *TableKey, value *any) bool,
) ([]*TableKey, []*any) {

	var tableKeys []*TableKey
	var resultValues []*any

	pool := NewTaskPool()

	db.RawIterKV(tableKey, func(key IKey, value []byte) (stop bool) {
		valCopy := value
		keyCopy := key.(*TableKey)
		pool.AddTask(func() {
			curObj, err := db.marshaller.Decode(valCopy)
			if err != nil {
				return
			}
			if predicate(keyCopy, curObj) {
				tableKeys = append(tableKeys, keyCopy)
				resultValues = append(resultValues, curObj)
			}
		})

		return false
	})

	pool.Close()

	return tableKeys, resultValues
}

//region Trigger

func (db *KVStoreManager) runBeforeTriggers(
	operation Operation,
	key IKey,
	value *any,
) bool {

	ok := true

	pool := NewTaskPool()

	for _, trig := range db.triggers {

		if trig.IsBefore() == true &&
			trig.TableName() == StructName(*value) &&
			trig.Operation().Contain(operation) {

			trigCopy := trig
			pool.AddTask(func() {
				ok = ok && trigCopy.StartBefore(operation, key, value)
			})
		}
	}

	pool.Close()

	return ok
}

func (db *KVStoreManager) runAfterTriggers(
	operation Operation,
	key IKey,
	value *any,
) {
	pool := NewTaskPool()

	for _, trig := range db.triggers {

		if trig.IsBefore() == false &&
			trig.TableName() == StructName(*value) &&
			trig.Operation().Contain(operation) {

			trigCopy := trig
			pool.AddTask(func() {
				trigCopy.StartAfter(operation, key, value)
			})
		}
	}

	pool.Close()
}

func (db *KVStoreManager) withTriggerWrapper(
	key IKey,
	value *any,
	operation Operation,
	action func() error,
) error {

	if !db.runBeforeTriggers(operation, key, value) {
		return ErrCancelledByTrigger
	}

	if err := action(); err != nil {
		return err
	}

	db.runAfterTriggers(operation, key, value)

	return nil
}

//endregion
