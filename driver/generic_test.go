package driver_test

import (
	"encoding/gob"
	. "github.com/Phosmachina/FluentKV/core"
	. "github.com/Phosmachina/FluentKV/driver"
	. "github.com/Phosmachina/FluentKV/helper"
	"strconv"
	"testing"
)

// region Object type declaration

type SimpleType struct {
	T1  string
	T2  string
	Val int
}

func NewSimpleType(t1 string, t2 string, val int) *SimpleType {
	return &SimpleType{T1: t1, T2: t2, Val: val}
}

type AnotherType struct {
	T3      string
	Numeric float32
}

func NewAnotherType(t3 string, numeric float32) *AnotherType {
	anotherType := &AnotherType{T3: t3, Numeric: numeric}
	return anotherType
}

// endregion

type DriverTester struct {
	*testing.T
	db       *KVStoreManager
	setUp    func(*DriverTester)
	tearDown func(*DriverTester)
}

func NewDriverTester(t *testing.T) *DriverTester {

	// Register the type used in db.
	gob.Register(SimpleType{})
	gob.Register(AnotherType{})

	return &DriverTester{T: t}
}

func (i *DriverTester) SetSetUp(setUp func(*DriverTester)) *DriverTester {
	i.setUp = setUp
	return i
}

func (i *DriverTester) SetTearDown(tearDown func(*DriverTester)) *DriverTester {
	i.tearDown = tearDown
	return i
}

func (i *DriverTester) SetDb(db *KVStoreManager) {
	i.db = db
}

func (i *DriverTester) RunAllTests() {

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
		i.T.Run(Name(test), func(t *testing.T) {
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

func (i *DriverTester) TestRawSet(t *testing.T) {

	value := any(NewSimpleType("val for t1", "val for t2", 42))

	encoded, err := i.db.Marshaller().Encode(&value)
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

func (i *DriverTester) TestRawGet(t *testing.T) {

	value := any(NewSimpleType("val for t1", "val for t2", 42))

	encoded, err := i.db.Marshaller().Encode(&value)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	tableKey := NewTableKey[SimpleType]().SetId("0")
	i.db.RawSet(tableKey, encoded)

	rawResult, _ := i.db.RawGet(tableKey)

	resultValue, err := i.db.Marshaller().Decode(rawResult)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if !Equals(*resultValue, value) {
		t.Error("Value not equals after GET.")
	}
}

func (i *DriverTester) TestRawDelete_Existant(t *testing.T) {

	value := any(NewSimpleType("val for t1", "val for t2", 42))

	encoded, err := i.db.Marshaller().Encode(&value)
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

func (i *DriverTester) TestRawDelete_Inexistant(t *testing.T) {
	if i.db.RawDelete(NewTableKey[AnotherType]().SetId("42")) {
		t.Error("Inexistant entry marked as deleted.")
	}
}

func (i *DriverTester) TestRawIterKey(t *testing.T) {

	var values []*SimpleType
	for i := 0; i < 4; i++ {
		values = append(values, NewSimpleType("val for t1", "val for t2", i))
	}

	var expectedTableKeys []*TableKey
	for id, value := range values {
		valueAsAny := any(value)
		encoded, err := i.db.Marshaller().Encode(&valueAsAny)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		tableKey := NewTableKey[SimpleType]().SetId(strconv.Itoa(id))
		i.db.RawSet(tableKey, encoded)
		expectedTableKeys = append(expectedTableKeys, tableKey)
	}

	i.db.RawIterKey(NewTableKey[SimpleType](), func(key IKey) (stop bool) {

		found := RemoveWithPredicate(&expectedTableKeys, key.(*TableKey),
			func(k1 *TableKey, k2 *TableKey) bool { return k1.Equals(k2) })

		if !found {
			t.Errorf("Unexpected key when iterate: %s", key)
			t.FailNow()
		}

		return false
	})
}

func (i *DriverTester) TestRawIterKV(t *testing.T) {

	var objects []*SimpleType
	for i := 0; i < 4; i++ {
		objects = append(objects, NewSimpleType("val for t1", "val for t2", i))
	}

	expectedKV := map[string]*SimpleType{}
	for id, value := range objects {
		valueAsAny := any(value)
		encoded, err := i.db.Marshaller().Encode(&valueAsAny)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		tableKey := NewTableKey[SimpleType]().SetId(strconv.Itoa(id))
		i.db.RawSet(tableKey, encoded)
		expectedKV[tableKey.Id()] = value
	}

	i.db.RawIterKV(NewTableKey[SimpleType](), func(key IKey, value []byte) (stop bool) {

		expectedObj, ok := expectedKV[key.(*TableKey).Id()]
		if !ok {
			t.Errorf("Unexpected key when iterate: %s", key)
			t.FailNow()
		}

		decoded, err := i.db.Marshaller().Decode(value)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		ToString(expectedObj)
		ToString(decoded)

		if !Equals(*expectedObj, *decoded) {
			t.Error("Object is not equal to the expected one")
			t.FailNow()
		}

		return false
	})
}

func (i *DriverTester) TestExist_Existant(t *testing.T) {

	value := any(NewSimpleType("val for t1", "val for t2", 42))

	encoded, err := i.db.Marshaller().Encode(&value)
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

func (i *DriverTester) TestExist_Inexistant(t *testing.T) {
	if i.db.Exist(NewTableKey[AnotherType]().SetId("42")) {
		t.Error("Inexistant entry marked as existant.")
	}
}

func TestGeneric(t *testing.T) {
	NewDriverTester(t).
		SetSetUp(func(tester *DriverTester) {
			tester.SetDb(NewGeneric())
		}).
		RunAllTests()
}
