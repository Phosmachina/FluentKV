package reldb

import (
	"errors"
	"reflect"
)

type RelDB interface {
	Insert(object *Object) *ObjWrapper
	Set(id string, object *Object) *ObjWrapper
	Get(tableName string, id string) *ObjWrapper
	Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper
	Delete(tableName string, id string) error
	DeepDelete(tableName string, id string) error
	Exist(tableName string, id string) (bool, error)

	Count(tableName string) (int, error)
	Foreach(tableName string, do func(objWrapper ObjWrapper)) error
	FindFirst(tableName string, predicate func(objWrapper ObjWrapper) bool) (*ObjWrapper, error)
	FindAll(tableName string, predicate func(objWrapper ObjWrapper) bool) ([]*ObjWrapper, error)

	Print(tableName string) error
}

type Object interface {
	Hash() string
	ToString() string
	TableName() string
}

type ObjWrapper struct {
	db    *RelDB
	ID    string
	Value *Object
}

func NewObjWrapper() *ObjWrapper {
	return &ObjWrapper{}
}

func (t *ObjWrapper) Link(ids ...string) error {

	return nil
}

func (t *ObjWrapper) LinkNew(objs ...*Object) error {
	return nil
}

func (t *ObjWrapper) DirectionalLink(ids ...string) error {
	return nil
}

func (t *ObjWrapper) DirectionalLinkNew(objs ...*Object) error {
	return nil
}

func (t *ObjWrapper) FromLink(objType string, id string) *ObjWrapper {
	return nil
}

func (t *ObjWrapper) AllFromLink(objType string) []*ObjWrapper {
	return nil
}

//region Helpers

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

func Type[T Object]() *T {
	return (*T)(nil)
}

//endregion
