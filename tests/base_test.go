package tests

import (
	"encoding/gob"
	. "github.com/Phosmachina/FluentKV/core"
	. "github.com/Phosmachina/FluentKV/helper"
	. "github.com/Phosmachina/FluentKV/implementation"
)

// region DB Object type declaration

type SimpleType struct {
	DBObject
	T1  string
	T2  string
	Val int
}

func NewSimpleType(t1 string, t2 string, val int) SimpleType {
	simpleType := SimpleType{T1: t1, T2: t2, Val: val}
	simpleType.IObject = simpleType
	return simpleType
}

func (t SimpleType) ToString() string  { return ToString(t) }
func (t SimpleType) TableName() string { return NameOfStruct[SimpleType]() }

type AnotherType struct {
	DBObject
	T3      string
	Numeric float32
}

func NewAnotherType(t3 string, numeric float32) AnotherType {
	anotherType := AnotherType{T3: t3, Numeric: numeric}
	anotherType.IObject = anotherType
	return anotherType
}

func (t AnotherType) ToString() string  { return ToString(t) }
func (t AnotherType) TableName() string { return NameOfStruct[AnotherType]() }

// endregion

// prepareTest clean previous data, register the DB type for Gob, create a new DB.
func prepareTest(dir string) IRelationalDB {

	// Register the type used in db
	gob.Register(SimpleType{})
	gob.Register(AnotherType{})

	// Initialize db
	db, _ := NewBadgerDB(dir)

	return db
}
