package tests

import (
	"encoding/gob"
	. "git.haythor.ml/naxxar/relational-badger/impl"
	. "git.haythor.ml/naxxar/relational-badger/reldb"
	"os"
	"testing"
)

type SimpleType struct {
	DBObject
	T1  string
	T2  string
	Val int
}

func (t SimpleType) ToString() string {
	return ToString(t)
}

func prepareTest() IRelationalDB {
	// Clean previous db data
	_ = os.RemoveAll("data")

	// Register type used in db
	gob.Register(DBObject{})
	gob.Register(SimpleType{})

	// Initialize db
	AutoKeyBuffer = 10
	db, _ := NewBadgerDB("data")
	return db
}

func Test_InsertGet_SimpleType(t *testing.T) {
	object := NewAbstractObject(SimpleType{
		T1:  "val for t1",
		T2:  "val for t2",
		Val: 42,
	})

	db := prepareTest()

	objWrapper := db.Insert(IObject(object))
	if !objWrapper.Value.Equals(object) {
		t.Error("Value not equal after INSERT.")
	}

	get := db.Get(object.TableName(), "0")
	t.Log(get.Value.ToString())
	if !get.Value.Equals(object) {
		t.Error("Values not equals after GET.")
	}
}

func Test_Update_SimpleType(t *testing.T) {
	object := NewAbstractObject(SimpleType{
		T1:  "val for t1",
		T2:  "val for t2",
		Val: 42,
	})

	db := prepareTest()

	insert := db.Insert(IObject(object))
	t.Log(insert.Value.ToString())

	db.Update(object.TableName(), "0", func(objWrapper *ObjWrapper) {
		simpleType := objWrapper.Value.(SimpleType)
		simpleType.Val = 24
		objWrapper.Value = IObject(simpleType)
	})
}
