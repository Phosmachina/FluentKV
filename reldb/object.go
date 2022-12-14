package reldb

import "github.com/kataras/golog"

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

func (t *ObjWrapper) Link(tableName string, ids ...string) {
	for _, id := range ids {
		exist := t.db.Exist(tableName, id)
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", id)
			continue
		}
		k := MakeLinkKey(t, tableName, id)
		t.db.RawSet(PrefixLink, k[0], nil)
		t.db.RawSet(PrefixLink, k[1], nil)
	}
}

func (t *ObjWrapper) LinkNew(objs ...*IObject) []error {
	return nil
}

func (t *ObjWrapper) DirectionalLink(tableName string, ids ...string) error {
	return nil
}

func (t *ObjWrapper) DirectionalLinkNew(tableName string, objs ...*IObject) error {
	return nil
}

func (t *ObjWrapper) FromLink(tableName string, id string) *ObjWrapper {
	return nil
}

func (t *ObjWrapper) AllFromLink(tableName string) []*ObjWrapper {
	return nil
}
