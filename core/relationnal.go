package core

import (
	"errors"
)

var (
	AutoKeyBuffer = 1000

	// PreAutoKavlb prefix for available key.
	PreAutoKavlb = "tank%avlb_"
	// PreAutoKused prefix for used keys.
	PreAutoKused = "tank%used_"

	ErrInvalidId         = errors.New("the id specified is nod used in the db")
	ErrFailedToSet       = errors.New("the set operation failed")
	ErrSelfBind          = errors.New("try to link object to itself")
	ErrDuplicateTrigger  = errors.New("the trigger with same id for this table name is already added")
	ErrInexistantTrigger = errors.New("the trigger does not exist")
)

/*
IRelationalDB is a small interface to define some operation with a storage used like a relational DB.

# Key

At first, the underlying storage should work like a KV DB. In consequence,
a key is structured to store a flattened hierarchy.
  - Key: internalTypePrefix, suffix
  - Key for concrete types: internalTypePrefix, tableName, id

To make uniq key, a tank key system is implemented and can be used with GetKey,
FreeKey. The global AutoKeyBuffer defined the size of this tank. When the value is inserted,
a key is picked in the tank. After deleting an entry, the key becomes available again via GetKey.

# Operators

Interface is designed so that all raw operators must be implemented; other can be but are already
implemented in the abstraction AbstractRelDB. Raw Operators probably work directly with the db driver and are used by all other operators.
*/
type IRelationalDB interface {
	// GetKey pick a key in the tank of unique keys.
	//If the tank is empty, it will be filled with new keys.
	GetKey() string
	// FreeKey check if the key is used and make the key available again.
	FreeKey(key ...string)
	// Close, set the db to the closed state and finally close the db.
	Close()

	// RawSet set a value in DB. Prefix and key are simply concatenated.
	// Don't care about Key is already in use or not. Return false when the operation failed.
	RawSet(prefix string, key string, value []byte) bool
	// RawGet get a value in DB. Prefix and key are simply concatenated.
	// If no value corresponding to this Key, empty slice and false should be returned.
	RawGet(prefix string, key string) ([]byte, bool)
	// RawDelete delete a value in DB. Prefix and key are simply concatenated.
	// Return true if value is correctly deleted.
	RawDelete(prefix string, key string) bool
	// RawIterKey iterate in DB when prefix match with Key.
	// The action it called for each Key and the key is truncated with the prefix given.
	// The stop boolean defined if iteration should be stopped.
	// No values are prefetched with this iterator.
	RawIterKey(prefix string, action func(key string) (stop bool))
	// RawIterKV iterate in DB when prefix match with Key.
	// The action it called for each Key and the key is truncated with the prefix given.
	// The stop boolean defined if iteration should be stopped.
	// Value is the corresponding value of the key.
	RawIterKV(prefix string, action func(key string, value []byte) (stop bool))

	// Insert creates a new entry in storage with IObject passed. TableName is inferred with the IObject.
	Insert(object IObject) (string, []error)
	// Set write a value for a specific id.
	// TableName is inferred with the IObject.
	// If the Key does not exist, an error is returned.
	Set(id string, object IObject) []error
	SetWrp(objWrp ObjWrapper[IObject]) []error
	// Get retrieve the value for the corresponding TableName and ID.
	// Return nil if nothing is found.
	Get(tableName string, id string) (*IObject, []error)
	// Update retrieves the value for corresponding TableName and ID,
	// call the editor et Set the resulted value.
	Update(tableName string, id string, editor func(value IObject) IObject) (*IObject, []error)
	// Delete remove the value for corresponding TableName and ID. If Key not exist,
	// an error is returned. The link using the object will be also deleted.
	Delete(tableName string, id string) []error
	// DeepDelete remove the value for corresponding TableName and ID. If Key not exist,
	// an error is returned. It also recursively remove all values connected with a link.
	DeepDelete(tableName string, id string) []error
	// Exist return true if the for corresponding TableName and ID exist in DB.
	Exist(tableName string, id string) bool

	// Count return the count for matching Key prefixed by TableName that exist in DB.
	Count(tableName string) int
	// Foreach call the do function for each value whose key is prefixed by TableName.
	Foreach(tableName string, do func(id string, value *IObject))
	// FindFirst iterate on values of tableName and apply the predicate: if predicate is true the
	// value is returned.
	FindFirst(tableName string, predicate func(id string, value *IObject) bool) (string, *IObject)
	// FindAll iterate on values of tableName and apply the predicate: all values matched are
	// returned.
	FindAll(tableName string, predicate func(id string, value *IObject) bool) ([]string, []*IObject)

	// getAbstractRelDB allow retrieving the underlying AbstractRelDB for inner
	// operations.
	getAbstractRelDB() *AbstractRelDB
}
