package reldb

import (
	"github.com/kataras/golog"
	"strings"
)

type IObject interface {
	Hash() string
	ToString() string
	TableName() string
}

type ObjWrapper struct {
	db    IRelationalDB
	ID    string
	Value *IObject
}

func NewObjWrapper(db IRelationalDB, ID string, value *IObject) *ObjWrapper {
	return &ObjWrapper{db: db, ID: ID, Value: value}
}

func (t *ObjWrapper) Link(biDirectional bool, tableName string, ids ...string) {
	for _, id := range ids {
		exist := t.db.Exist(tableName, id)
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", id)
			continue
		}
		k := MakeLinkKey(t, tableName, id)
		t.db.RawSet(PrefixLink, k[0], nil)
		if biDirectional {
			t.db.RawSet(PrefixLink, k[1], nil)
		}
	}
}

func (t *ObjWrapper) LinkNew(biDirectional bool, objs ...*IObject) {
	for _, obj := range objs {
		t.Link(biDirectional, (*t.Value).TableName(), t.db.Insert(obj).ID)
	}
}

func (t *ObjWrapper) FromLink(tableName string, id string) *ObjWrapper {
	K := MakeLinkKey(t, tableName, id)
	rawGet, found := t.db.RawGet(PrefixLink, K[0])
	if found {
		return NewObjWrapper(t.db, id, Decode(rawGet))
	}
	return nil
}

func (t *ObjWrapper) AllFromLink(tableName string) []*ObjWrapper {
	var objs []*ObjWrapper
	t.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, (*t.Value).TableName()+Delimiter+t.ID+LinkDelimiter+tableName) {
			objs = append(objs, t.db.Get(
				tableName,
				strings.Split(strings.Split(key, LinkDelimiter)[1], Delimiter)[1],
			))
		}
		return false
	})
	return objs
}
