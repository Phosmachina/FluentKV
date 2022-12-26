package reldb

import (
	"crypto/md5"
	"fmt"
	"github.com/kataras/golog"
	"strings"
	"unsafe"
)

type IObject interface {
	Equals(v IObject) bool
	Hash() string
	ToString() string
	TableName() string
}

type DBObject struct{ IObject }

func (o DBObject) Equals(v IObject) bool {
	return o.Hash() == v.Hash()
}

func (o DBObject) Hash() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(o.ToString())))
}

type ObjWrapper[T IObject] struct {
	db    IRelationalDB
	ID    string
	Value T
}

func NewObjWrapper[T IObject](db IRelationalDB, ID string, value *T) *ObjWrapper[T] {
	return &ObjWrapper[T]{db: db, ID: ID, Value: *value}
}

func (t *ObjWrapper[IObject]) Link(biDirectional bool, tableName string, ids ...string) {
	for _, id := range ids {
		exist := t.db.Exist(tableName, id)
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", id)
			continue
		}
		k := MakeLinkKey(t.Value.TableName(), t.ID, tableName, id)
		if biDirectional {
			t.db.RawSet(PrefixLink, k[1], nil)
		}
		t.db.RawSet(PrefixLink, k[0], nil)
	}
}

func (t *ObjWrapper[IObject]) LinkNew(biDirectional bool, objs ...IObject) []*ObjWrapper[IObject] {
	var objWrapped []*ObjWrapper[IObject]
	for _, obj := range objs {
		id := t.db.Insert(obj)
		t.Link(biDirectional, (t.Value).TableName(), id)
		wrapper := NewObjWrapper(t.db, id, &obj)
		objWrapped = append(objWrapped, wrapper)
	}
	return objWrapped
}

func (t *ObjWrapper[IObject]) FromLink(tableName string) (string, *IObject) {
	ids, objects := t.AllFromLink(tableName)
	if len(ids) == 0 {
		return "", nil
	}
	return ids[0], objects[0]
}

func (t *ObjWrapper[IObject]) AllFromLink(tableName string) ([]string, []*IObject) {
	var resultIds []string
	var resultValues []*IObject
	t.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, t.Value.TableName()+Delimiter+t.ID+LinkDelimiter+tableName) {
			resultIds = append(resultIds, key)
			resultValues = append(resultValues,
				(*IObject)(unsafe.Pointer(t.db.Get(
					tableName,
					strings.Split(strings.Split(key, LinkDelimiter)[1], Delimiter)[1]))),
			)
		}
		return false
	})
	return resultIds, resultValues
}

// region Fluent toolkit

//func Link[T IObject](t *ObjWrapper[IObject], biDirectional bool, tableName string, ids ...string) {
//	t.Link(biDirectional, tableName, ids...)
//}
//
//func LinkNew[T IObject](t *ObjWrapper[IObject], biDirectional bool, objs ...IObject) {
//	t.LinkNew(biDirectional, objs...)
//}
//
//func FromLink[T IObject](t *ObjWrapper[IObject], tableName string, id string) *ObjWrapper[T] {
//	resultId, resultValue := t.FromLink(tableName, id)
//	if resultValue != nil {
//		return NewObjWrapper(t.db, resultId, (*T)(unsafe.Pointer(resultValue)))
//	}
//	return nil
//}
//
//func AllFromLink[T IObject](t *ObjWrapper[IObject], tableName string) []*ObjWrapper[T] {
//	var objs []*ObjWrapper[T]
//	ids, results := t.AllFromLink(tableName)
//	for i, curId := range ids {
//		objs = append(objs, NewObjWrapper(t.db, curId, (*T)(unsafe.Pointer(results[i]))))
//	}
//	return objs
//}

//endregion
