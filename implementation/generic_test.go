package implementation_test

import (
	"encoding/gob"
	. "github.com/Phosmachina/FluentKV/core"
	. "github.com/Phosmachina/FluentKV/helper"
	. "github.com/Phosmachina/FluentKV/implementation"
	"reflect"
	"runtime"
	"strconv"
	"testing"
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

type ImplementationTester struct {
	*testing.T
	db       IRelationalDB
	setUp    func(*ImplementationTester)
	tearDown func(*ImplementationTester)
}

func NewImplementationTester(t *testing.T) *ImplementationTester {

	// Register the type used in db
	gob.Register(SimpleType{})
	gob.Register(AnotherType{})

	return &ImplementationTester{T: t}
}

func (i *ImplementationTester) SetSetUp(setUp func(*ImplementationTester)) *ImplementationTester {
	i.setUp = setUp
	return i
}

func (i *ImplementationTester) SetTearDown(tearDown func(*ImplementationTester)) *ImplementationTester {
	i.tearDown = tearDown
	return i
}

func (i *ImplementationTester) SetDb(db IRelationalDB) {
	i.db = db
}

func (i *ImplementationTester) Name(f func(*testing.T)) string {
	pc := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
	return pc.Name()
}

func (i *ImplementationTester) RunAllTests() {
	tests := []func(*testing.T){
		i.TestRawSet,
		i.TestRawGet,
		i.TestRawDelete_Existant,
		i.TestRawDelete_Inexistant,
		i.TestRawIterKey,
		i.TestRawIterKV,
		i.TestExist_Existant,
		i.TestExist_Inexistant,
	}

	for _, test := range tests {
		i.T.Run(i.Name(test), func(t *testing.T) {
			if i.setUp != nil {
				i.setUp(i)
			}
			test(t)
			if i.tearDown != nil {
				i.tearDown(i)
			}
		})
	}
}

func (i *ImplementationTester) TestRawSet(t *testing.T) {

	object := NewSimpleType("val for t1", "val for t2", 42)

	iObject := IObject(object)
	encoded, err := i.db.Marshaller().Encode(&iObject)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	tableKey := NewTableKey[SimpleType]().SetId("0")
	i.db.RawSet(tableKey, encoded)

	_, exist := i.db.RawGet(tableKey)

	if !exist {
		t.Error("Value not found after SET.")
	}

}

func (i *ImplementationTester) TestRawGet(t *testing.T) {

	object := NewSimpleType("val for t1", "val for t2", 42)

	iObject := IObject(object)
	encoded, err := i.db.Marshaller().Encode(&iObject)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	tableKey := NewTableKey[SimpleType]().SetId("0")
	i.db.RawSet(tableKey, encoded)

	get, _ := i.db.RawGet(tableKey)

	decoded, err := i.db.Marshaller().Decode(get)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if !(*decoded).(SimpleType).Equals(object) {
		t.Error("Value not equals after GET.")
	}
}

func (i *ImplementationTester) TestRawDelete_Existant(t *testing.T) {

	object := NewSimpleType("val for t1", "val for t2", 42)

	iObject := IObject(object)
	encoded, err := i.db.Marshaller().Encode(&iObject)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	tableKey := NewTableKey[SimpleType]().SetId("0")
	i.db.RawSet(tableKey, encoded)

	if !i.db.RawDelete(tableKey) {
		t.Error("Marked as not deleted after correct DELETE")
	}

	if i.db.Exist(tableKey) {
		t.Error("Marked as existing after DELETE")
	}

}

func (i *ImplementationTester) TestRawDelete_Inexistant(t *testing.T) {
	if i.db.RawDelete(NewTableKey[AnotherType]().SetId("42")) {
		t.Error("Inexistant entry marked as deleted.")
	}
}

func (i *ImplementationTester) TestRawIterKey(t *testing.T) {

	var objects []SimpleType
	for i := 0; i < 4; i++ {
		objects = append(objects, NewSimpleType("val for t1", "val for t2", i))
	}

	var expectedTableKeys []TableKey
	for id, object := range objects {
		iObject := IObject(object)
		encoded, err := i.db.Marshaller().Encode(&iObject)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		tableKey := NewTableKey[SimpleType]().SetId(strconv.Itoa(id))
		i.db.RawSet(tableKey, encoded)
		expectedTableKeys = append(expectedTableKeys, tableKey)
	}

	i.db.RawIterKey(NewTableKey[SimpleType](), func(key IKey) (stop bool) {

		found := false
		for _, expectedKey := range expectedTableKeys {
			if key.(TableKey).Equals(expectedKey) {
				expectedTableKeys = Remove(expectedTableKeys, expectedKey)
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Unexpected key when iterate: %s", key)
			t.FailNow()
		}

		return false
	})
}

func (i *ImplementationTester) TestRawIterKV(t *testing.T) {

	var objects []SimpleType
	for i := 0; i < 4; i++ {
		objects = append(objects, NewSimpleType("val for t1", "val for t2", i))
	}

	expectedKV := map[TableKey]SimpleType{}
	for id, object := range objects {
		iObject := IObject(object)
		encoded, err := i.db.Marshaller().Encode(&iObject)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		tableKey := NewTableKey[SimpleType]().SetId(strconv.Itoa(id))
		i.db.RawSet(tableKey, encoded)
		expectedKV[tableKey] = object
	}

	i.db.RawIterKV(NewTableKey[SimpleType](), func(key IKey, value []byte) (stop bool) {

		expectedObj, ok := expectedKV[key.(TableKey)]
		if !ok {
			t.Errorf("Unexpected key when iterate: %s", key)
			t.FailNow()
		}

		decoded, err := i.db.Marshaller().Decode(value)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if !expectedObj.Equals((*decoded).(SimpleType)) {
			t.Error("Object is not equal to the expected one")
			t.FailNow()
		}

		return false
	})
}

func (i *ImplementationTester) TestExist_Existant(t *testing.T) {

	object := NewSimpleType("val for t1", "val for t2", 42)

	iObject := IObject(object)
	encoded, err := i.db.Marshaller().Encode(&iObject)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	tableKey := NewTableKey[SimpleType]().SetId("0")
	i.db.RawSet(tableKey, encoded)

	if !i.db.Exist(tableKey) {
		t.Error("Existant entry marked as inexistant.")
	}

}

func (i *ImplementationTester) TestExist_Inexistant(t *testing.T) {
	if i.db.Exist(NewTableKey[AnotherType]().SetId("42")) {
		t.Error("Inexistant entry marked as existant.")
	}
}

func TestGeneric(t *testing.T) {
	NewImplementationTester(t).
		SetSetUp(func(tester *ImplementationTester) {
			tester.SetDb(NewGeneric())
		}).
		RunAllTests()
}
