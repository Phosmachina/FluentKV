package reldb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/kataras/golog"
	"reflect"
)

var (
	AutoKeyBuffer = 100

	// PreAutoKavlb prefix for available key.
	PreAutoKavlb = "tank%avlb_"
	// PreAutoKused prefix for used key.
	PreAutoKused = "tank%used_"
	// PrefixLink prefix for a link declaration.
	PrefixLink = "link%"
	// PrefixTable prefix for a table entry.
	PrefixTable = "tbl%"

	// Delimiter between the tableName and the key
	Delimiter     = "_"
	LinkDelimiter = "@"

	InvalidId = errors.New("InvalidId")
)

/*
IRelationalDB is a small interface to define some operation with a storage used like a relational DB.

# Key

In first, the underlying storage should be work like a KV DB. In consequence,
a key is structured to store a flattened hierarchy.
  - Key: internalTypePrefix, suffix
  - Key for concrete type: internalTypePrefix, tableName, id

To make uniq key, a tank key system is implemented and can be used with GetNextKey,
FreeKey. The global AutoKeyBuffer defined the size of this tank. When value is inserted,
a key is pick in tank. When entry is deleted, the key become available again via GetNextKey.

# Operators

Interface is designed so that all raw operator must be implemented ; other can be but are already
implement in the abstraction AbstractRelDB. Raw Operators probably work directly with the db driver and are used by all other operators.
*/
type IRelationalDB interface {
	// GetNextKey pick a key in tank of key. If tank is empty, it should be filled with unused key.
	GetNextKey() string
	// FreeKey check if key is used (return error otherwise) and make key available again.
	FreeKey(key ...string) []error
	CleanUnusedKey()

	// RawSet set a value in DB. prefix and key are simply concatenated. Don't care about Key is already in used or not.
	RawSet(prefix string, key string, value []byte)
	// RawGet get a value in DB. prefix and key are simply concatenated. If no value corresponding to this Key,
	//empty slice and false should be returned.
	RawGet(prefix string, key string) ([]byte, bool)
	// RawDelete delete a value in DB. prefix and key are simply concatenated. Return true if value is correctly deleted.
	RawDelete(prefix string, key string) bool
	// RawIterKey iterate in DB when prefix match with Key.
	//The action it called for each Key and the key is truncated with the prefix given.
	//The stop boolean defined if iteration should be stopped. No values are prefetched with this iterator.
	RawIterKey(prefix string, action func(key string) (stop bool))
	// RawIterKV iterate in DB when prefix match with Key.
	//The action it called for each Key and the key is truncated with the prefix given.
	//The stop boolean defined if iteration should be stopped. value is the corresponding value of the key.
	RawIterKV(prefix string, action func(key string, value []byte) (stop bool))

	// Insert create a new entry in storage with IObject passed. TableName is inferred with the IObject.
	Insert(object IObject) string
	// Set write a value for a specific id. TableName is inferred with the IObject. If Key not exist, an error is returned.
	Set(id string, object IObject) error
	SetWrp(objWrp ObjWrapper[IObject]) error
	// Get retrieve the value for corresponding TableName and ID. Return nil if nothing found.
	Get(tableName string, id string) *IObject
	// Update retrieve the value for corresponding TableName and ID, call the editor et Set the resulted value.
	Update(tableName string, id string, editor func(value IObject) IObject) *IObject
	// Delete remove the value for corresponding TableName and ID. If Key not exist,
	//an error is returned. The link using the object will be also deleted.
	Delete(tableName string, id string) error
	// DeepDelete remove the value for corresponding TableName and ID. If Key not exist,
	//an error is returned. It also recursively remove all values connected with a link.
	DeepDelete(tableName string, id string) error
	// Exist return true if the for corresponding TableName and ID exist in DB.
	Exist(tableName string, id string) bool

	// Count return the count for matching Key prefixed by TableName that exist in DB.
	Count(tableName string) int
	// Foreach call the do function for each value whose key is prefixed by TableName.
	Foreach(tableName string, do func(id string, value *IObject))
	// FindFirst iterate on values of tableName and apply the predicate: if predicate is true the
	//value is returned.
	FindFirst(tableName string, predicate func(id string, value *IObject) bool) (string, *IObject)
	// FindAll iterate on values of tableName and apply the predicate: all values matched are
	//returned.
	FindAll(tableName string, predicate func(id string, value *IObject) bool) ([]string, []*IObject)

	Print(tableName string) error
}

//region Helpers

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
		golog.Error(err)
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
		golog.Error(err)
	}
	return object
}

//endregion

//region Utils

// NameOfStruct simply reflect the name of the type T.
func NameOfStruct[T any]() string {
	return reflect.TypeOf((*T)(nil)).Elem().Name()
}

func NameOfField(parent interface{}, field interface{}) (string, error) {
	s := reflect.ValueOf(parent).Elem()
	f := reflect.ValueOf(field).Elem()
	for i := 0; i < s.NumField(); i++ {
		valueField := s.Field(i)
		if valueField.Addr().Interface() == f.Addr().Interface() {
			return s.Type().Field(i).Name, nil
		}
	}
	return "", errors.New("invalid parameters")
}

func Type[T IObject]() *T {
	return (*T)(nil)
}

// ToString print the name of type and all field name with the corresponding value.
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

//endregion
