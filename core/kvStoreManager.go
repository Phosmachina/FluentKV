package core

import (
	"errors"
	. "github.com/Phosmachina/FluentKV/helper"
	"strconv"
	"sync"
)

var (
	// AutoIdBuffer defines the default batch size used for pre-allocated IDs.
	// When IDs are required, a range of AutoIdBuffer IDs are added to the available set.
	AutoIdBuffer = 1000

	// ErrInvalidId indicates that a provided ID is not valid or is not found in the DB.
	ErrInvalidId = errors.New("the Id specified is not used in the db")

	// ErrFailedToSet indicates that a Set or Insert operation failed at the storage driver layer.
	ErrFailedToSet = errors.New("the set operation failed")

	// ErrSelfBind is an error used for link or relational logic, implying an object is linking to itself.
	ErrSelfBind = errors.New("try to link object to itself")
)

// KVStoreManager provides a higher-level manager on top of a KVDriver to handle both
// raw primitive operations (like RawSet, RawGet, RawDelete) and higher-level tasks such
// as ID generation and marshaling.
// The final user will typically use these features indirectly, through the fluent API in fluent.go.
type KVStoreManager struct {
	// KVDriver represents the underlying database driver that the manager extends.
	KVDriver

	// marshaller performs encoding/decoding of objects to/from byte slices, enabling
	// typed operations to be stored in a key-value format.
	marshaller IMarshaller

	// availableIds is the internal pool of IDs that are not currently in use.
	// It’s populated during initialization and replenished as needed.
	availableIds []string

	// usedIds holds the set of IDs that are actively assigned in the store.
	usedIds []string

	// triggers is an optional set of ITrigger hooks that can be run before or after
	// specific CRUD operations.
	triggers []ITrigger

	m sync.Mutex
}

// NewKVStoreManager initializes a KVStoreManager based on the given driver.
// It scans the existing store (via KVDriver.RawIterKey) to determine which IDs are in use,
// and it constructs an initial pool of available IDs up to AutoIdBuffer.
func NewKVStoreManager(driver KVDriver) *KVStoreManager {

	kvStoreManager := KVStoreManager{
		KVDriver:   driver,
		marshaller: &GobMarshaller{}, // Default marshaller for objects.
	}

	biggestId := 0

	// Gather in-use IDs from the underlying storage.
	driver.RawIterKey(NewProtoTableKey(), func(key IKey) (stop bool) {
		id := key.(*TableKey).Id()
		kvStoreManager.usedIds = append(kvStoreManager.usedIds, id)

		idAsInt, _ := strconv.Atoi(id)
		if biggestId < idAsInt {
			biggestId = idAsInt
		}

		return false
	})

	// Pre-populate available IDs up to a range that safely encompasses current usage.
	for i := 0; i < AutoIdBuffer*(1+biggestId/AutoIdBuffer); i++ {
		key := strconv.Itoa(i)
		if IndexOf(key, kvStoreManager.usedIds) == -1 {
			kvStoreManager.availableIds = append(kvStoreManager.availableIds, key)
		}
	}

	return &kvStoreManager
}

// SetMarshaller assigns a custom marshaller to the manager.
// This is useful if you want to swap out the default GobMarshaller for another implementation.
func (db *KVStoreManager) SetMarshaller(marshaller IMarshaller) *KVStoreManager {
	db.marshaller = marshaller
	return db
}

// Marshaller retrieves the manager’s current marshaller.
func (db *KVStoreManager) Marshaller() IMarshaller {
	return db.marshaller
}

// GetFreeId fetches a free identifier from the pool (availableIds).
// If the pool is empty, it repopulates it by adding a new batch of IDs.
// The chosen ID is transferred to the usedIds list.
// If an ID is not used in the store, it will be freed up automatically at the next start.
func (db *KVStoreManager) GetFreeId() string {

	db.m.Lock()
	defer db.m.Unlock()

	// If no IDs are available, create a new batch.
	if len(db.availableIds) == 0 {
		for i := len(db.usedIds); i < len(db.usedIds)+AutoIdBuffer; i++ {
			db.availableIds = append(db.availableIds, strconv.Itoa(i))
		}
	}

	id := db.availableIds[0]
	db.usedIds = append(db.usedIds, id)
	db.availableIds = db.availableIds[1:]

	return id
}

// FreeId reclaims one or more IDs (passed as variadic arguments) by removing
// them from the usedIds list and adding them back to the availableIds pool.
// This method is typically called whenever an object is deleted from the store.
func (db *KVStoreManager) FreeId(ids ...string) {

	db.m.Lock()
	defer db.m.Unlock()

	for _, id := range ids {
		if index := IndexOf(id, db.usedIds); index != -1 {
			// Remove the ID from usedIds
			db.usedIds = append(db.usedIds[:index], db.usedIds[index+1:]...)

			// Place ID back in availableIds
			db.availableIds = append(db.availableIds, id)
		}
	}
}

// Insert encodes the given value (as *any) using the current marshaller and inserts it
// into the underlying driver using a newly allocated key.
// If insertion fails, the allocated ID is freed.
// Triggers are run if defined.
func (db *KVStoreManager) Insert(value *any) (*TableKey, error) {

	tableKey := NewTableKeyFromObject(*value).SetId(db.GetFreeId())

	errs := db.withTriggerWrapper(tableKey, value, InsertOperation, func() error {
		encoded, err := db.marshaller.Encode(value)
		if err != nil {
			return err
		}

		if !db.RawSet(tableKey, encoded) {
			return ErrFailedToSet
		}
		return nil
	})

	if errs != nil {
		db.FreeId(tableKey.Id())
	}

	return tableKey, errs
}

// Set updates the record corresponding to tableKey with a newly encoded representation
// of the provided value.
// If the key does not exist in the store, ErrInvalidId is returned.
// Triggers are run if defined.
func (db *KVStoreManager) Set(tableKey *TableKey, value *any) error {

	if !db.Exist(tableKey) {
		return ErrInvalidId
	}

	return db.withTriggerWrapper(tableKey, value, UpdateOperation, func() error {
		encoded, err := db.marshaller.Encode(value)
		if err != nil {
			return err
		}
		if !db.RawSet(tableKey, encoded) {
			return ErrFailedToSet
		}
		return nil
	})
}

// Get retrieves a record specified by tableKey, decodes it (using the current marshaller),
// and returns the resulting object as *any.
// If the key does not exist, ErrInvalidId is returned.
// Triggers run if defined.
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

// Update retrieves the current object matching tableKey, runs the user-provided editor
// function to modify it in memory, then encodes and re-saves it.
// If the key does not exist, ErrInvalidId is returned.
// If the final writing step fails, ErrFailedToSet is raised.
// Triggers run if defined.
func (db *KVStoreManager) Update(tableKey *TableKey, editor func(value *any) *any) (*any, error) {

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
		rawUpdatedValue, encodeErr := db.marshaller.Encode(value)
		if encodeErr != nil {
			return encodeErr
		}
		if !db.RawSet(tableKey, rawUpdatedValue) {
			return ErrFailedToSet
		}
		return nil
	})

	return value, err
}

// Delete removes the record associated with the given tableKey.
// Before removing, it fetches the value for triggers or auditing, then reclaims its ID.
// Any links referencing the deleted item are also removed.
// If the key does not exist, ErrInvalidId is returned.
// Triggers run if defined.
func (db *KVStoreManager) Delete(tableKey *TableKey) error {

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

		// Remove all links referencing this key.
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

// DeepDelete removes the record and all directly connected entries, recursively.
// This is similar to Delete but also calls DeepDelete on any linked object.
// If the key does not exist, ErrInvalidId is returned.
// Triggers run if defined.
func (db *KVStoreManager) DeepDelete(tableKey *TableKey) error {

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

		// Recursively remove links and linked objects.
		db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {
			linkKey := key.(*LinkKey)

			if linkKey.CurrentTableKey().Equals(tableKey) {
				db.RawDelete(linkKey)
				_ = db.DeepDelete(linkKey.TargetTableKey())
			} else if linkKey.TargetTableKey().Equals(tableKey) {
				db.RawDelete(linkKey)
			}

			return false
		})

		return nil
	})
}

// Count returns the number of entries in the store whose keys match the tableKey prefix.
// This is effectively counting the records in a particular “table” domain.
func (db *KVStoreManager) Count(key *TableKey) int {
	var ct int
	db.RawIterKey(key, func(_ IKey) (stop bool) {
		ct++
		return false
	})
	return ct
}

// Foreach iterates over all key-value pairs matching the given tableKey prefix.
// For each match, it decodes the value and invokes the provided callback function.
// This allows you to process each entry without manually managing iteration or lookups.
func (db *KVStoreManager) Foreach(
	tableKey *TableKey,
	do func(tableKey *TableKey, value *any),
) {

	pool := NewTaskPool()

	db.RawIterKV(tableKey, func(key IKey, rawValue []byte) (stop bool) {
		valCopy := rawValue
		keyCopy := key.(*TableKey)
		pool.AddTask(func() {
			decoded, err := db.marshaller.Decode(valCopy)
			if err != nil {
				return
			}
			do(keyCopy, decoded)
		})
		return false
	})

	pool.Close()
}

// FindFirst scans the store for all items matching the provided tableKey prefix,
// decodes them, and checks each against a given predicate. If the predicate returns
// true, iteration stops, and the function returns that item’s tableKey and value.
//
// If no match is found, it returns (nil, nil).
func (db *KVStoreManager) FindFirst(
	tableKey *TableKey,
	predicate func(tableKey *TableKey, value *any) bool,
) (*TableKey, *any) {

	var resultKey *TableKey
	var resultValue *any

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

// FindAll iterates over every item matching the tableKey prefix, decodes each value,
// and collects those that match a user-supplied predicate.
// The function returns the list of matching keys and their associated objects.
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
