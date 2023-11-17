package tests

import (
	"fmt"
	. "github.com/Phosmachina/FluentKV/reldb"
	"testing"
)

func Test_NewCollection(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	db := prepareTest(t.TempDir())

	for _, obj := range objs {
		Insert(db, obj)
	}

	collection := NewCollection[SimpleType](db)
	for i, objWrp := range collection.GetArray() {
		if !objWrp.Value.Equals(objs[i]) {
			t.Error("Array from collection are not equal to the original array.")
			break
		}
	}
}

func Test_Sort(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	db := prepareTest(t.TempDir())

	for _, obj := range objs {
		Insert(db, obj)
	}

	collection := NewCollection[SimpleType](db)
	collection.Sort(func(x, y *ObjWrapper[SimpleType]) bool {
		return x.Value.Val > y.Value.Val
	})
	for i, objWrp := range collection.GetArray() {
		if !objWrp.Value.Equals(objs[(len(objs)-1)-i]) {
			t.Error("Objects are not properly ordered.")
		}
	}
}

func Test_Distinct(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 0),
		NewSimpleType("duplicate1", "duplicate1", 1),
		NewSimpleType("duplicate1", "duplicate1", 1),
		NewSimpleType("duplicate2", "duplicate2", 2),
		NewSimpleType("duplicate2", "duplicate2", 2),
	}
	db := prepareTest(t.TempDir())

	for _, obj := range objs {
		Insert(db, obj)
	}

	collection := NewCollection[SimpleType](db)
	collection.Distinct()

	if collection.Len() != 3 {
		t.Fatal("The collection has not the expected count of objects after Distinct.")
	}
	for i, objWrp := range collection.GetArray() {
		if !objWrp.Value.Equals(objs[i*2]) {
			t.Fatal("Not expected value, the arrays is corrupted or not in correct order after Distinct.")
		}
	}
}

func Test_Where(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 0),
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
	}
	objsTarget := []AnotherType{
		NewAnotherType("", 10.3),
		NewAnotherType("", 15),
		NewAnotherType("", 7.12),
	}
	db := prepareTest(t.TempDir())

	for i, obj := range objs {
		objWrp := Insert(db, obj)
		LinkNew(objWrp, true, objsTarget[i])
	}

	collection := NewCollection[SimpleType](db)
	Where[SimpleType, AnotherType](
		true,
		collection,
		func(objWrp1 *ObjWrapper[SimpleType], objWrp2 *ObjWrapper[AnotherType]) bool {
			return objWrp2.Value.Numeric > 10
		},
	)
	if collection.Len() != 2 {
		t.Fatal("The collection has not the expected count of objects after Where.")
	}
	for i, o := range collection.GetArray() {
		if !o.Value.Equals(objs[i]) {
			t.Fatal("Not expected value, " +
				"the arrays is corrupted or not in correct order after Where.")
		}
	}
}

func Test_Filter(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	db := prepareTest(t.TempDir())

	for _, obj := range objs {
		Insert(db, obj)
	}

	collection := NewCollection[SimpleType](db)
	collection.Filter(func(objWrp *ObjWrapper[SimpleType]) bool {
		return objWrp.Value.Val <= 1
	})
	if len(collection.GetArray()) != 1 {
		t.Fatal("The collection has not the expected amount of element after the filter.")
	}
	if !collection.GetArray()[0].Value.Equals(objs[0]) {
		t.Fatal("The value of the collection is not what is expected.")
	}
}

// //////////////////////////////////////////////////////////////////////////////////////////////////

// TODO test complex cases based on SQL relevant situations (find them on https://sql.sh).

func Test_SubQueries(t *testing.T) {

}

// //////////////////////////////////////////////////////////////////////////////////////////////////

func Benchmark_NewCollection(b *testing.B) {

	db := prepareTest(b.TempDir())
	for i := 0; i < b.N; i++ {
		Insert(db, NewSimpleType("val for t1", "val for t2", 986))
	}

	fmt.Println("New test with:", b.N, "it")
	b.ResetTimer()

	NewCollection[SimpleType](db)

	b.Cleanup(db.Close)
}
