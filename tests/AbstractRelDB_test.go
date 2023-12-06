package tests

import (
	"errors"
	. "github.com/Phosmachina/FluentKV/reldb"
	"testing"
)

func Test_AddTriggerForInsert(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest(t.TempDir())

	work := false
	err := AddBeforeTrigger(db, "my trigger", InsertOperation,
		func(id string, value *SimpleType) error {
			work = true
			if Count[SimpleType](db) != 0 {
				t.Error(
					"Invalid number of SimpleType: should 0 because trigger is called before.")
			}
			return nil
		})
	if err != nil {
		t.FailNow()
	}

	var idInserted string
	err = AddAfterTrigger(db, "check id", InsertOperation,
		func(id string, value *SimpleType) {
			idInserted = id
			if Count[SimpleType](db) != 1 {
				t.Error(
					"Invalid number of SimpleType: should 1 because trigger is called after.")
			}
		})
	if err != nil {
		t.FailNow()
	}

	simpleTypeWrp, _ := Insert(db, object)

	if !work {
		t.Error("The flag is not raised after operation.")
	}
	if simpleTypeWrp.ID != idInserted {
		t.Error("Id read in trigger function is invalid.")
	}
}

func Test_AddTriggerForGet(t *testing.T) {
	//db := prepareTest(t.TempDir())

}

func Test_AddTriggerForUpdate(t *testing.T) {
	//db := prepareTest(t.TempDir())

}

func Test_AddTriggerForDelete(t *testing.T) {
	//db := prepareTest(t.TempDir())

}

func Test_AddTriggerForMultipleOperation(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest(t.TempDir())

	work := false
	err := AddBeforeTrigger(db, "my trigger", InsertOperation|DeleteOperation,
		func(id string, value *SimpleType) error {
			work = true
			return nil
		})
	if err != nil {
		t.FailNow()
	}

	simpleTypeWrp, _ := Insert(db, object)
	if !work {
		t.Error("The flag is not raised after Insert.")
	}
	work = false

	_ = Delete[SimpleType](db, simpleTypeWrp.ID)
	if !work {
		t.Error("The flag is not raised after Delete.")
	}
}

func Test_DeleteTrigger(t *testing.T) {
	object := NewSimpleType("val for t1", "val for t2", 42)
	db := prepareTest(t.TempDir())

	work := true
	_ = AddBeforeTrigger(db, "my trigger", InsertOperation,
		func(id string, value *SimpleType) error {
			work = false
			return nil
		})
	err := DeleteTrigger[SimpleType](db, "my trigger")
	if err != nil {
		t.FailNow()
	}

	Insert(db, object)

	if !work {
		t.Error("The flag is raised after operation.")
	}
}

func Test_AddExistingTrigger(t *testing.T) {
	db := prepareTest(t.TempDir())

	err := AddAfterTrigger[SimpleType](db, "id", GetOperation, nil)
	if err != nil {
		t.FailNow()
	}

	err = AddAfterTrigger[SimpleType](db, "id", GetOperation, nil)
	if !errors.Is(err, ErrDuplicateTrigger) {
		t.FailNow()
	}
}

func Test_DeleteInexistantTrigger(t *testing.T) {
	db := prepareTest(t.TempDir())

	err := DeleteTrigger[SimpleType](db, "id")
	if !errors.Is(err, ErrInexistantTrigger) {
		t.FailNow()
	}
}
