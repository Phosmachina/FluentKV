package reldb

import (
	"errors"
	"github.com/kataras/golog"
	"reflect"
)

type RelDB interface {
	RawSet(prefix string, key string, value []byte)

	Insert(object *Object) *ObjWrapper
	Set(id string, object *Object) *ObjWrapper
	Get(tableName string, id string) *ObjWrapper
	Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper
	Delete(tableName string, id string) error
	DeepDelete(tableName string, id string) error
	Exist(tableName string, id string) (bool, error)

	Count(tableName string) int
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
	db    RelDB
	ID    string
	Value *Object
}

func NewObjWrapper(db RelDB, value *Object) *ObjWrapper {
	return &ObjWrapper{db: db, Value: value}
}

func (t *ObjWrapper) Link(tableName string, ids ...string) error {
	for _, id := range ids {
		exist, err := t.db.Exist(tableName, id)
		if err != nil {
			return err
		}
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", id)
			continue
		}
		// TODO insert link to db
	}
	return nil
}

func (t *ObjWrapper) LinkNew(objs ...*Object) error {
	return nil
}

func (t *ObjWrapper) DirectionalLink(tableName string, ids ...string) error {
	return nil
}

func (t *ObjWrapper) DirectionalLinkNew(tableName string, objs ...*Object) error {
	return nil
}

func (t *ObjWrapper) FromLink(tableName string, id string) *ObjWrapper {
	return nil
}

func (t *ObjWrapper) AllFromLink(tableName string) []*ObjWrapper {
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
