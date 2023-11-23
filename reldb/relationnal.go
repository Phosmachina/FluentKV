package fluentkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"reflect"
)

var (
	AutoKeyBuffer = 1000

	// PreAutoKavlb prefix for available key.
	PreAutoKavlb = "tank%avlb_"
	// PreAutoKused prefix for used keys.
	PreAutoKused = "tank%used_"
	// PrefixLink prefix for a link declaration.
	PrefixLink = "link%"
	// PrefixTable prefix for a table entry.
	PrefixTable = "tbl%"

	// Delimiter between the tableName and the key
	Delimiter     = "_"
	LinkDelimiter = "@"

	ErrInvalidId         = errors.New("the id specified is nod used in the db")
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
a key is picked in the tank. When entry is deleted, the key becomes available again via GetKey.

# Operators

Interface is designed so that all raw operators must be implemented; other can be but are already

	implemented in the abstraction AbstractRelDB. Raw Operators probably work directly with the db driver and are used by all other operators.
*/
type IRelationalDB interface {
	// GetKey pick a key in tank of unique keys.
	//If the tank is empty, it will be filled with new keys.
	GetKey() string
	// FreeKey check if key is used and make the key available again.
	FreeKey(key ...string)
	// Close set the db to the closed state and finally close the db.
	Close()

	// RawSet set a value in DB. Prefix and key are simply concatenated.
	// Don't care about Key is already in use or not.
	RawSet(prefix string, key string, value []byte)
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
	Insert(object IObject) string
	// Set write a value for a specific id.
	// TableName is inferred with the IObject.
	// If the Key does not exist, an error is returned.
	Set(id string, object IObject) error
	SetWrp(objWrp ObjWrapper[IObject]) error
	// Get retrieve the value for the corresponding TableName and ID.
	// Return nil if nothing is found.
	Get(tableName string, id string) *IObject
	// Update retrieves the value for corresponding TableName and ID,
	// call the editor et Set the resulted value.
	Update(tableName string, id string, editor func(value IObject) IObject) *IObject
	// Delete remove the value for corresponding TableName and ID. If Key not exist,
	// an error is returned. The link using the object will be also deleted.
	Delete(tableName string, id string) error
	// DeepDelete remove the value for corresponding TableName and ID. If Key not exist,
	// an error is returned. It also recursively remove all values connected with a link.
	DeepDelete(tableName string, id string) error
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

// region Helpers

func MakePrefix(tableName string) string {
	return PrefixTable + tableName + Delimiter
}

func MakeKey(tableName, id string) []byte {
	return []byte(MakePrefix(tableName) + id)
}

func MakeLinkKey(tableName string, id string, targetName string, targetId string) []string {

	k1 := tableName + Delimiter + id
	k2 := targetName + Delimiter + targetId

	return []string{k1 + LinkDelimiter + k2, k2 + LinkDelimiter + k1}
}

func Encode(obj *IObject) []byte {

	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(obj)
	if err != nil {
		// TODO return err ; make some custom err
		log.Printf(err.Error())
		return nil
	}
	return buffer.Bytes()
}

func Decode(value []byte) *IObject {

	buffer := bytes.Buffer{}
	var object *IObject
	buffer.Write(value)
	err := gob.NewDecoder(&buffer).Decode(&object)
	if err != nil {
		return nil
		// TODO return nil/err ; make some custom err
	}

	return object
}

// endregion

// region Utils

// NameOfStruct simply reflect the name of the type T.
func NameOfStruct[T any]() string {
	return reflect.TypeOf((*T)(nil)).Elem().Name()
}

// ToString prints the name of type and all field names with the corresponding value.
func ToString(v any) string {

	typeOf := reflect.TypeOf(v)
	result := typeOf.Name()
	value := reflect.ValueOf(v)

	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		result += fmt.Sprintf(" | %s: %v", field.Name, value.Field(i))
	}

	return result
}

// IndexOf returns the index of the first occurrence of an element in the provided slice,
// or -1 if any element is not present in the slice.
func IndexOf[T comparable](element T, data []T) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 // not found.
}

// endregion
