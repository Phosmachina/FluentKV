package reldb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/kataras/golog"
	"reflect"
)

var (
	AutoKeyBuffer = 100

	PreAutoKavlb = "tank%avlb_"
	PreAutoKused = "tank%used_"
	PrefixLink   = "link%"
	PrefixTable  = "tbl%"

	Delimiter     = "_"
	LinkDelimiter = "@"
)

type IRelationalDB interface {
	GetNextKey() string
	FreeKey(key ...string) []error
	CleanUnusedKey()

	RawSet(prefix string, key string, value []byte)
	RawGet(prefix string, key string) ([]byte, bool)
	RawDelete(prefix string, key string) bool
	RawIterKey(prefix string, action func(key string) (stop bool))
	RawIterKV(prefix string, action func(key string, value []byte) (stop bool))

	Insert(object *IObject) *ObjWrapper
	Set(id string, object *IObject) *ObjWrapper
	Get(tableName string, id string) *ObjWrapper
	Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper
	Delete(tableName string, id string) error
	DeepDelete(tableName string, id string) error
	Exist(tableName string, id string) bool

	Count(tableName string) int
	Foreach(tableName string, do func(objWrapper ObjWrapper))
	FindFirst(tableName string, predicate func(objWrapper ObjWrapper) bool) *ObjWrapper
	FindAll(tableName string, predicate func(objWrapper ObjWrapper) bool) []*ObjWrapper

	Print(tableName string) error
}

//region Helpers

func MakePrefix(tableName string) string {
	return PrefixTable + tableName + Delimiter
}

func MakeKey(tableName, id string) []byte {
	return []byte(MakePrefix(tableName) + id)
}

func MakeLinkKey(obj *ObjWrapper, targetName string, targetId string) []string {
	k1 := (*obj.Value).TableName()
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
	err := gob.NewDecoder(&buffer).Decode(object)
	if err != nil {
		golog.Error(err)
	}
	return object
}

//endregion

//region Utils

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

//endregion
