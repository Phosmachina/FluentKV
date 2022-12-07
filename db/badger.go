package db

import . "Relational_Badger/reldb"

type BadgerDB struct {
}

func NewBadgerDB() *BadgerDB {
	return &BadgerDB{}
}

//region RelDB implementation

func (b *BadgerDB) Insert(object *Object) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Set(id string, object *Object) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Get(tableName string, id string) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Update(tableName string, id string, editor func(objWrapper *ObjWrapper)) *ObjWrapper {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Delete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) DeepDelete(tableName string, id string) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Exist(tableName string, id string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Count(tableName string) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Foreach(tableName string, do func(objWrapper ObjWrapper)) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) FindFirst(tableName string, predicate func(objWrapper ObjWrapper) bool) (
	*ObjWrapper, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) FindAll(tableName string, predicate func(objWrapper ObjWrapper) bool) (
	[]*ObjWrapper, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerDB) Print(tableName string) error {
	//TODO implement me
	panic("implement me")
}

//endregion
