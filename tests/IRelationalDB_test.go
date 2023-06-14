package tests

import (
	. "git.antlia.tk/naxxar/FluentKV/reldb"
	"strconv"
	"testing"
)

func Test_RawSetRawGet_SimpleType(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	iObject := IObject(object)
	db.RawSet("tbl%SimpleType_", "0", Encode(&iObject))
	get, exist := db.RawGet("tbl%SimpleType_", "0")

	if !exist {
		t.Error("Value not found after SET.")
	}

	decoded := (*Decode(get)).(SimpleType)
	if !decoded.Equals(object) {
		t.Error("Value not equals after GET.")
	}
}

func Test_InsertGet_SimpleType(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	insertedValue := Insert(db, object)
	if !insertedValue.Value.Equals(object) {
		t.Error("Value not equal after INSERT.")
	}

	getResult := Get[SimpleType](db, insertedValue.ID)
	if !getResult.Value.Equals(object) {
		t.Error("Values not equals after GET.")
	}
}

func Test_Set_SimpleType(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	id := Insert(db, object).ID
	object.Val = 12
	Set(db, id, object)

	if Get[SimpleType](db, id).Value.Val != 12 {
		t.Error("Invalid value after SET.")
	}
}

func Test_Update_SimpleType(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	id := Insert(db, object).ID
	Update(db, id, func(value *SimpleType) {
		value.Val = 12
	})

	value := Get[SimpleType](db, id).Value

	if value.Val != 12 {
		t.Error("Values not equals after GET.")
	}
}

func Test_Exist(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	id := Insert(db, object).ID

	if !Exist[SimpleType](db, id) {
		t.Error("Entry not exist after INSERT.")
	}
	if db.Exist("SimpleType", "1") {
		t.Error("Invalid entry exist.")
	}
}

func Test_Count(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest()

	Insert(db, object)

	count := Count[SimpleType](db)
	if count != 1 {
		t.Error("Count is invalid after INSERT one value.")
	}
	if db.Count("Invalid") != 0 {
		t.Error("Count is invalid on invalid table name.")
	}
}

func Test_Link(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)
	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)

	o3Wrp := LinkNew(o1Wrp, false, o3)[0]
	Link(o1Wrp, true, o2Wrp)

	if len(AllFromLinkWrp[SimpleType, SimpleType](o3Wrp)) > 0 {
		t.Error("Unlink return something in place of nothing.")
	}

	results := AllFromLinkWrp[SimpleType, SimpleType](o1Wrp)
	if len(results) == 0 {
		t.Error("Unlink return nothing in place of o2 object.")
	}
	if !results[0].Value.Equals(o2) {
		t.Error("Unlink not return o2 object.")
	}
}

func Test_Delete(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	db := prepareTest()

	idO2 := LinkNew(Insert(db, o1), true, o2)[0].ID

	err := Delete[SimpleType](db, idO2)
	if err != nil {
		t.Error("Failed to delete inserted o2 object.")
	}

	if Get[SimpleType](db, idO2) != nil {
		t.Error("The deleted o2 object still present.")
	}
}

func Test_DeepDelete_0(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)
	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)

	Link(o1Wrp, true, o2Wrp)
	Link(o1Wrp, false, o3Wrp)

	err := DeepDelete[SimpleType](db, o1Wrp.ID)
	if err != nil {
		t.Error("DeepDelete not correctly done.")
	}

	if Get[SimpleType](db, o1Wrp.ID) != nil {
		t.Error("o1 object not deleted.")
	}
	if Get[SimpleType](db, o2Wrp.ID) != nil {
		t.Error("o2 object not deleted.")
	}
	if Get[SimpleType](db, o3Wrp.ID) != nil {
		t.Error("o3 object not deleted.")
	}
}

func Test_DeepDelete_1(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)
	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)

	Link(o3Wrp, true, o2Wrp)
	Link(o1Wrp, false, o3Wrp)

	err := DeepDelete[SimpleType](db, o3Wrp.ID)
	if err != nil {
		t.Error("DeepDelete not correctly done.")
	}
	if Get[SimpleType](db, o1Wrp.ID) == nil {
		t.Error("o1 object is deleted.")
	}
	if Get[SimpleType](db, o2Wrp.ID) != nil {
		t.Error("o2 object not deleted.")
	}
	if Get[SimpleType](db, o3Wrp.ID) != nil {
		t.Error("o3 object not deleted.")
	}
}

func Test_RemoveLink(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewAnotherType("t3", 1.1)
	o3 := NewAnotherType("val for t3", 4.5)

	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)

	Link(o1Wrp, true, o2Wrp)
	Link(o1Wrp, true, o3Wrp)

	RemoveLinkWrp(o2Wrp, o1Wrp)

	if !AllFromLinkWrp[SimpleType, AnotherType](o1Wrp)[0].Value.Equals(o3Wrp.Value) {
		t.Error("Invalid link after link deletion.")
	}
	if len(AllFromLinkWrp[SimpleType, SimpleType](o1Wrp)) != 0 {
		t.Error("Link are not deleted.")
	}
}

func Test_RemoveAllTableLink(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)

	o4 := NewAnotherType("val for t3", 4.5)
	o5 := NewAnotherType("val for t3", 5.5)

	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)
	o4Wrp := Insert(db, o4)
	o5Wrp := Insert(db, o5)

	Link(o1Wrp, true, o2Wrp)
	Link(o1Wrp, true, o3Wrp)
	Link(o1Wrp, true, o4Wrp)
	Link(o1Wrp, true, o5Wrp)

	RemoveAllTableLinkWrp[SimpleType, AnotherType](o1Wrp)

	if obj := AllFromLinkWrp[SimpleType, AnotherType](o1Wrp); len(obj) != 0 {
		t.Error("Links AnotherType are not deleted correctly.")
	}
	if obj := AllFromLinkWrp[AnotherType, SimpleType](o4Wrp); len(obj) != 0 {
		t.Error("Links AnotherType are not deleted correctly.")
	}
	if obj := AllFromLinkWrp[SimpleType, SimpleType](o1Wrp); len(obj) != 2 {
		t.Error("Links of SimpleType are deleted.")
	}
}

func Test_RemoveAllLink(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)

	o4 := NewAnotherType("val for t3", 4.5)
	o5 := NewAnotherType("val for t3", 5.5)

	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)
	o4Wrp := Insert(db, o4)
	o5Wrp := Insert(db, o5)

	Link(o1Wrp, true, o2Wrp)
	Link(o1Wrp, true, o3Wrp)
	Link(o1Wrp, true, o4Wrp)
	Link(o1Wrp, true, o5Wrp)

	RemoveAllLinkWrp(o1Wrp)

	if len(AllFromLinkWrp[SimpleType, AnotherType](o1Wrp)) != 0 || len(AllFromLinkWrp[SimpleType,
		SimpleType](o1Wrp)) != 0 {
		t.Error("All link are not deleted.")
	}
}

func Test_Foreach(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	db := prepareTest()

	for _, obj := range objs {
		Insert(db, obj)
	}

	Foreach(db, func(id string, value *SimpleType) {
		i, _ := strconv.Atoi(id)
		if !objs[i].Equals(value) {
			t.Error("Not expected value in foreach.")
		}
	})
}

func Test_FindFirst(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	db := prepareTest()

	for _, obj := range objs {
		Insert(db, obj)
	}
	first := FindFirst(db, func(id string, value *SimpleType) bool {
		return value.Val == 2
	})
	if first == nil {
		t.Error("FindFirst found nothing.")
	}
	if !objs[1].Equals(first.Value) {
		t.Error("FindFirst found other than th second item.")
	}
}

func Test_FindAll(t *testing.T) {
	objs := []SimpleType{
		NewSimpleType("val for t1", "val for t2", 1),
		NewSimpleType("val for t1", "val for t2", 2),
		NewSimpleType("val for t1", "val for t2", 3),
	}
	common := []SimpleType{
		NewSimpleType("specific", "val for t2", 1),
		NewSimpleType("specific", "val for t2", 2),
	}
	db := prepareTest()

	for _, obj := range objs {
		Insert(db, obj)
	}
	for _, obj := range common {
		Insert(db, obj)
	}

	all := FindAll(db, func(id string, value *SimpleType) bool {
		return value.T1 == "specific"
	})

	if len(all) != len(common) {
		t.Fatal("FindAll not return the expected count of results.")
	}
	for i, obj := range all {
		if !obj.Value.Equals(common[i]) {
			t.Error("FindAll return an unexpected value.")
		}
	}
}

func Test_Visit(t *testing.T) {
	o1 := NewSimpleType("val for t1", "val for t2", 1)
	o2 := NewSimpleType("val for t1", "val for t2", 2)
	o3 := NewSimpleType("val for t1", "val for t2", 3)

	o4 := NewAnotherType("val for t3", 4.5)
	o5 := NewAnotherType("val for t3", 5.5)

	db := prepareTest()

	o1Wrp := Insert(db, o1)
	o2Wrp := Insert(db, o2)
	o3Wrp := Insert(db, o3)
	_ = DeleteWrp(o3Wrp)
	o4Wrp := Insert(db, o4)
	o5Wrp := Insert(db, o5)

	Link(o1Wrp, true, o2Wrp)
	Link(o1Wrp, true, o4Wrp)
	Link(o1Wrp, false, o5Wrp)

	i := len(VisitWrp[SimpleType, AnotherType](o1Wrp))
	s := VisitWrp[AnotherType,
		SimpleType](o4Wrp)[0]
	if i != 2 || s != o1Wrp.ID {
		t.Error("Visit return an unexpected value.")
	}
	if len(VisitWrp[AnotherType, SimpleType](o5Wrp)) != 0 {
		t.Error("Visit return an unexpected value.")
	}
	rawPrint(db)
}

func rawPrint(db IRelationalDB) {
	db.RawIterKey("", func(key string) (stop bool) {
		println(key)
		return false
	})
}
