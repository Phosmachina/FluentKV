package core_test

import (
	"encoding/gob"
	"errors"
	. "github.com/Phosmachina/FluentKV/core"
	"github.com/Phosmachina/FluentKV/driver"
	. "github.com/Phosmachina/FluentKV/helper"
	"strconv"
	"testing"
)

func prepareTestableDb() *KVStoreManager {

	// Register types used in DB.
	gob.Register(SimpleType{})
	gob.Register(AnotherType{})

	// Use generic implementation for testing.
	return NewKVStoreManager(driver.NewGeneric())
}

func checkObjectWrapper(
	t *testing.T,
	objectWrp KVWrapper[SimpleType],
	expectedKey *TableKey,
	expectedObject *any,
) {
	if objectWrp.IsEmpty() {
		t.Errorf("Unexpected empty KVWrapper")
		return
	}

	if !objectWrp.Key().Equals(expectedKey) {
		t.Errorf(
			"Unexpected key in KVWrapper: expected %v, got %v",
			expectedKey,
			objectWrp.Key(),
		)
	}

	if !Equals(*objectWrp.Value(), *expectedObject) {
		t.Errorf(
			"Unexpected value in KVWrapper: expected %v, got %v",
			expectedObject,
			objectWrp.Value(),
		)
	}
}

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

//region Base

func TestFluentInsert(t *testing.T) {
	checkInsert(
		t,
		nil,
		func(db *KVStoreManager, object *SimpleType) (KVWrapper[SimpleType], error) {
			return Insert(db, object)
		},
	)
}

func TestFluentSet_ValidId(t *testing.T) {
	CheckSetValidId(
		t,
		nil,
		func(
			db *KVStoreManager,
			id string,
			value *SimpleType,
		) (KVWrapper[SimpleType], error) {
			return Set(db, id, value)
		},
	)
}

func TestFluentSet_InvalidId(t *testing.T) {
	CheckSetInvalidId(
		t,
		nil,
		func(
			db *KVStoreManager,
			id string,
			value *SimpleType,
		) (KVWrapper[SimpleType], error) {
			return Set(db, id, value)
		},
	)
}

func TestFluentGet_ValidId(t *testing.T) {
	CheckGetValidId(
		t,
		nil,
		func(db *KVStoreManager, id string) (KVWrapper[SimpleType], error) {
			return Get[SimpleType](db, id)
		},
	)
}

func TestFluentGet_InvalidId(t *testing.T) {
	CheckGetInvalidId(
		t,
		nil,
		func(db *KVStoreManager, id string) (KVWrapper[SimpleType], error) {
			return Get[SimpleType](db, id)
		},
	)
}

func TestFluentUpdate(t *testing.T) {
	CheckUpdate(
		t,
		nil,
		func(
			db *KVStoreManager,
			id string,
			editor func(value *SimpleType),
		) (KVWrapper[SimpleType], error) {
			return Update[SimpleType](db, id, editor)
		},
	)
}

func TestFluentDelete_ValidId(t *testing.T) {
	CheckDeleteValidId(
		t,
		nil,
		func(db *KVStoreManager, id string) error {
			return Delete[SimpleType](db, id)
		},
	)
}

func TestFluentDelete_InvalidId(t *testing.T) {
	CheckDeleteInvalidId(
		t,
		nil,
		func(db *KVStoreManager, id string) error {
			return Delete[SimpleType](db, id)
		},
	)
}

func TestFluentDeepDelete(t *testing.T) {
	CheckDeepDelete(
		t,
		nil,
		func(db *KVStoreManager, id string) error {
			return DeepDelete[SimpleType](db, id)
		},
	)
}

func TestFluentExist(t *testing.T) {
	CheckExist(
		t,
		nil,
		func(db *KVStoreManager, id string) bool {
			return Exist[SimpleType](db, id)
		},
	)
}

func TestFluentCount(t *testing.T) {
	CheckCount(
		t,
		nil,
		func(db *KVStoreManager) int {
			return Count[SimpleType](db)
		},
	)
}

func TestFluentForEach(t *testing.T) {
	CheckForEach(
		t,
		nil,
		func(db *KVStoreManager, action func(IKey, *SimpleType)) {
			Foreach[SimpleType](db, action)
		},
	)
}

func TestFluentFindFirst(t *testing.T) {
	CheckFindFirst(
		t,
		nil,
		func(db *KVStoreManager, predicate func(
			*TableKey,
			*SimpleType,
		) bool) (KVWrapper[SimpleType], error) {
			return FindFirst[SimpleType](db, predicate), nil
		},
	)
}

func TestFluentFindAll(t *testing.T) {
	CheckFindAll(
		t,
		nil,
		func(db *KVStoreManager, predicate func(*TableKey, *SimpleType) bool) []KVWrapper[SimpleType] {
			return FindAll[SimpleType](db, predicate)
		},
	)
}

func TestFluentSetWrp(t *testing.T) {
	CheckSetWrp(
		t,
		nil,
		func(db *KVStoreManager, wrp KVWrapper[SimpleType]) (KVWrapper[SimpleType], error) {
			return SetWrp(wrp)
		},
	)
}

func TestFluentDeleteWrp(t *testing.T) {
	CheckDeleteWrp(
		t,
		nil,
		func(db *KVStoreManager, wrp KVWrapper[SimpleType]) error {
			return DeleteWrp(wrp)
		},
	)
}

func TestFluentDeepDeleteWrp(t *testing.T) {
	CheckDeepDeleteWrp(
		t,
		nil,
		func(db *KVStoreManager, wrp KVWrapper[SimpleType]) error {
			return DeepDeleteWrp(wrp)
		},
	)
}

func TestFluentExistWrp(t *testing.T) {
	CheckExistWrp(
		t,
		nil,
		func(db *KVStoreManager, wrp KVWrapper[SimpleType]) bool {
			return ExistWrp(wrp)
		},
	)
}

//endregion

//region Links

func TestLink(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewSimpleType("t1", "t2", 1))
	target1, _ := Insert(db, NewAnotherType("t3", 1.1))
	target2, _ := Insert(db, NewAnotherType("t3", 2.2))
	targets := []KVWrapper[AnotherType]{target1, target2}

	// Act
	err := Link(current, false, targets...)

	// Assert
	if err != nil {
		t.Errorf("Link failed: expected %v, got %v", nil, err)
	}

	allConnectedToTarget := AllFromLink[AnotherType, SimpleType](db, targets[0].Key().Id())
	if len(allConnectedToTarget) != 0 {
		t.Errorf("Expecting %v connected to target, got %d", 0, len(allConnectedToTarget))
	}

	allConnectedToCurrent := AllFromLink[SimpleType, AnotherType](db, current.Key().Id())
	if len(allConnectedToCurrent) != 2 {
		t.Errorf("Expecting %v connected to target, got %d", 2, len(allConnectedToCurrent))
		t.FailNow()
	}
	for _, expectedObject := range targets {
		if !RemoveWithPredicate(
			&allConnectedToCurrent,
			expectedObject,
			func(o1 KVWrapper[AnotherType], o2 KVWrapper[AnotherType]) bool {
				return Equals(o1.Value(), o2.Value())
			},
		) {
			t.Errorf("AllFromLink failed: expected %v", expectedObject)
		}
	}
}

func TestLink_BiDirectional(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewSimpleType("t1", "t2", 1))
	target1, _ := Insert(db, NewAnotherType("t3", 1.1))
	target2, _ := Insert(db, NewAnotherType("t3", 2.2))
	targets := []KVWrapper[AnotherType]{target1, target2}

	// Act
	err := Link(current, true, targets...)

	// Assert
	if err != nil {
		t.Errorf("Link failed: expected %v, got %v", nil, err)
	}

	allConnectedToTarget := AllFromLink[AnotherType, SimpleType](db, targets[0].Key().Id())
	if len(allConnectedToTarget) != 1 {
		t.Errorf("Expecting %v connected to target, got %d", 1, len(allConnectedToTarget))
	}
	if !Equals(allConnectedToTarget[0].Value(), current.Value()) {
		t.Errorf("Link failed: expected %v, got %v", current.Value(), allConnectedToTarget[0].Value())
	}

	allConnectedToCurrent := AllFromLink[SimpleType, AnotherType](db, current.Key().Id())
	if len(allConnectedToCurrent) != 2 {
		t.Errorf("Expecting %v connected to target, got %d", 2, len(allConnectedToCurrent))
		t.FailNow()
	}
	for _, expectedObject := range targets {
		if !RemoveWithPredicate(
			&allConnectedToCurrent,
			expectedObject,
			func(o1 KVWrapper[AnotherType], o2 KVWrapper[AnotherType]) bool {
				return Equals(o1.Value(), o2.Value())
			},
		) {
			t.Errorf("AllFromLink failed: expected %v", expectedObject)
		}
	}
}

func TestLink_SelfBind(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewSimpleType("t1", "t2", 1))

	// Act
	err := Link(current, false, current)

	// Assert
	if !errors.Is(err, ErrSelfBind) {
		t.Errorf("Link failed: expected %v, got %v", ErrSelfBind, err)
	}
}

func TestLink_InvalidCurrent(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	target, _ := Insert(db, NewAnotherType("t3", 1.1))
	simpleType := NewSimpleType("t1", "t2", 1)
	objectWrp := NewKVWrapper[SimpleType](db, NewTableKey[SimpleType]().SetId("42"), simpleType)

	// Act
	err := Link(objectWrp, false, target)

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("Link failed: expected %v, got %v", ErrInvalidId, err)
	}
}

func TestLink_InvalidTarget(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))
	simpleType := NewSimpleType("t1", "t2", 1)
	objectWrp := NewKVWrapper[SimpleType](db, NewTableKey[SimpleType]().SetId("42"), simpleType)

	// Act
	err := Link(current, false, objectWrp)

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("Link failed: expected %v, got %v", ErrInvalidId, err)
	}
}

func TestLinkNew(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))
	targetObject := NewSimpleType("t1", "t2", 1)

	// Act
	resultsObjWrp := LinkNew(current, false, targetObject)

	// Assert
	if len(resultsObjWrp) != 1 {
		t.Errorf("LinkNew failed: expected %v, got %v", 1, len(resultsObjWrp))
	}
	if !Equals(resultsObjWrp[0].Value(), targetObject) {
		t.Errorf("LinkNew failed: expected %v, got %v", targetObject, resultsObjWrp[0].Value())
	}
	objWrpConnectedToCurrent, err := Get[SimpleType](db, resultsObjWrp[0].Key().Id())
	if err != nil {
		t.Errorf("LinkNew failed: expected %v, got %v", nil, err)
	}
	if !Equals(objWrpConnectedToCurrent.Value(), targetObject) {
		t.Errorf("LinkNew failed: expected %v, got %v", targetObject, objWrpConnectedToCurrent.Value())
	}
}

func TestAllFromLink(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))

	for i := 0; i < 4; i++ {
		LinkNew(current, false, NewAnotherType("t3", float32(i)))
	}

	var expectedConnected []KVWrapper[SimpleType]
	for i := 0; i < 4; i++ {
		objWrp := LinkNew(current, false, NewSimpleType("t1", "t2", i))
		expectedConnected = append(expectedConnected, objWrp...)
	}

	// Act
	simpleTypeConnectedToCurrent := AllFromLink[AnotherType, SimpleType](db, current.Key().Id())

	// Assert
	if len(expectedConnected) != len(simpleTypeConnectedToCurrent) {
		t.Errorf("AllFromLink failed: expected %v, got %v", len(expectedConnected), len(simpleTypeConnectedToCurrent))
	}
	for _, expectedObject := range expectedConnected {
		if !RemoveWithPredicate(
			&simpleTypeConnectedToCurrent,
			expectedObject,
			func(o1 KVWrapper[SimpleType], o2 KVWrapper[SimpleType]) bool {
				return Equals(o1.Value().Val, o2.Value().Val)
			},
		) {
			t.Errorf("AllFromLink failed: expected %v, got %v", expectedObject, simpleTypeConnectedToCurrent)
		}
	}
}

func TestRemoveLink(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))
	target := LinkNew(current, false, NewSimpleType("t1", "t2", 1))[0]

	// Act
	isRemoved := RemoveLink[AnotherType, SimpleType](db, current.Key().Id(), target.Key().Id())

	// Assert
	if !isRemoved {
		t.Errorf("Remove failed: expected %v, got %v", true, isRemoved)
	}
	if len(AllFromLink[AnotherType, SimpleType](db, current.Key().Id())) != 0 {
		t.Error("Expecting no linked object")
	}
}

func TestRemoveAllTableLink(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))

	for i := 0; i < 4; i++ {
		LinkNew(current, false, NewAnotherType("t3", float32(i)))
	}

	var expectedConnected []KVWrapper[SimpleType]
	for i := 0; i < 4; i++ {
		objWrp := LinkNew(current, false, NewSimpleType("t1", "t2", i))
		expectedConnected = append(expectedConnected, objWrp...)
	}

	// Act
	RemoveAllTableLink[AnotherType, SimpleType](db, current.Key().Id())

	// Assert
	if len(AllFromLink[AnotherType, SimpleType](db, current.Key().Id())) != 0 {
		t.Error("Expecting no linked object")
	}
	if len(AllFromLink[AnotherType, AnotherType](db, current.Key().Id())) != 4 {
		t.Error("Expecting 4 linked object")
	}
}

func TestRemoveAllLink(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	current, _ := Insert(db, NewAnotherType("t3", 1.1))

	for i := 0; i < 4; i++ {
		LinkNew(current, false, NewAnotherType("t3", float32(i)))
	}

	var expectedConnected []KVWrapper[SimpleType]
	for i := 0; i < 4; i++ {
		objWrp := LinkNew(current, false, NewSimpleType("t1", "t2", i))
		expectedConnected = append(expectedConnected, objWrp...)
	}

	// Act
	RemoveAllLink[AnotherType](db, current.Key().Id())

	// Assert
	if len(AllFromLink[AnotherType, SimpleType](db, current.Key().Id())) != 0 {
		t.Error("Expecting no linked object")
	}
	if len(AllFromLink[AnotherType, AnotherType](db, current.Key().Id())) != 0 {
		t.Error("Expecting no linked object")
	}
}

//endregion

//region Triggers

func TestAddBeforeTrigger_ExistantTrigger(t *testing.T) {
}

func TestAddBeforeTrigger_DuplicateTrigger(t *testing.T) {
}

func TestAddAfterTrigger_ExistantTrigger(t *testing.T) {
}

func TestAddAfterTrigger_DuplicateTrigger(t *testing.T) {
}

func TestDeleteTrigger_ExistantTrigger(t *testing.T) {
}

func TestDeleteTrigger_InexistantTrigger(t *testing.T) {

	// Arrange
	db := prepareTestableDb()

	// Act
	err := DeleteTrigger[SimpleType](db, "42")

	// Assert
	if !errors.Is(err, ErrInexistantTrigger) {
		t.Errorf("Expecting ErrInexistantTrigger, got %v", err)
	}
}

func TestTrigger_BeforeAllOperation(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	triggerId := "test trigger"
	insertOperationOk := false
	getOperationOk := false
	updateOperationOk := false
	deleteOperationOk := false

	// Act
	err := AddBeforeTrigger(
		db,
		triggerId,
		InsertOperation|GetOperation|UpdateOperation|DeleteOperation,
		func(operation Operation, key IKey, value *SimpleType) bool {
			id := key.(*TableKey).Id()
			switch operation {
			case InsertOperation:
				insertOperationOk = !insertOperationOk &&
					!Exist[SimpleType](db, id)
			case GetOperation:
				getOperationOk = !getOperationOk
			case UpdateOperation:
				updateOperationOk = !updateOperationOk &&
					value.Val == 1
			case DeleteOperation:
				deleteOperationOk = !deleteOperationOk &&
					Exist[SimpleType](db, id)
			}
			return true
		},
	)

	objWrp, _ := Insert(db, NewSimpleType("t1", "t2", 1))
	_, _ = Get[SimpleType](db, objWrp.Key().Id())
	_, _ = Update(db, objWrp.Key().Id(), func(value *SimpleType) {
		value.Val = 3
	})
	_ = Delete[SimpleType](db, objWrp.Key().Id())

	// Assert
	if err != nil {
		t.Errorf("AddBeforeTrigger failed: %v", err)
	}
	if !insertOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected insertOperationOk, got %v", insertOperationOk)
	}
	if !getOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected getOperationOk, got %v", getOperationOk)
	}
	if !updateOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected updateOperationOk, got %v", updateOperationOk)
	}
	if !deleteOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected deleteOperationOk, got %v", deleteOperationOk)
	}
}

func TestTrigger_AfterAllOperation(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	triggerId := "test trigger"
	insertOperationOk := false
	getOperationOk := false
	updateOperationOk := false
	deleteOperationOk := false

	// Act
	err := AddAfterTrigger(
		db,
		triggerId,
		InsertOperation|GetOperation|UpdateOperation|DeleteOperation,
		func(operation Operation, key IKey, value *SimpleType) {
			id := key.(*TableKey).Id()
			switch operation {
			case InsertOperation:
				insertOperationOk = !insertOperationOk &&
					Exist[SimpleType](db, id)
			case GetOperation:
				getOperationOk = !getOperationOk
			case UpdateOperation:
				updateOperationOk = !updateOperationOk &&
					value.Val == 3
			case DeleteOperation:
				deleteOperationOk = !deleteOperationOk &&
					!Exist[SimpleType](db, id)
			}
		},
	)

	objWrp, _ := Insert(db, NewSimpleType("t1", "t2", 1))
	_, _ = Get[SimpleType](db, objWrp.Key().Id())
	_, _ = Update(db, objWrp.Key().Id(), func(value *SimpleType) {
		value.Val = 3
	})
	_ = Delete[SimpleType](db, objWrp.Key().Id())

	// Assert
	if err != nil {
		t.Errorf("AddBeforeTrigger failed: %v", err)
	}
	if !insertOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected insertOperationOk, got %v", insertOperationOk)
	}
	if !getOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected getOperationOk, got %v", getOperationOk)
	}
	if !updateOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected updateOperationOk, got %v", updateOperationOk)
	}
	if !deleteOperationOk {
		t.Errorf("AddBeforeTrigger failed: expected deleteOperationOk, got %v", deleteOperationOk)
	}
}

func TestTrigger_MultipleBeforeInsert(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	triggerCount := 0

	// Act
	for i := 0; i < 5; i++ {
		_ = AddBeforeTrigger[SimpleType](
			db,
			"trigger"+strconv.Itoa(i),
			InsertOperation,
			func(operation Operation, key IKey, value *SimpleType) bool {
				triggerCount++
				return true
			},
		)
	}

	_, _ = Insert(db, NewSimpleType("t1", "t2", 1))

	// Assert
	if triggerCount != 5 {
		t.Errorf("AddBeforeTrigger failed: expected 5, got %d", triggerCount)
	}
}

func TestTrigger_MultipleAfterInsert(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	triggerCount := 0

	// Act
	for i := 0; i < 5; i++ {
		_ = AddAfterTrigger[SimpleType](
			db,
			"trigger"+strconv.Itoa(i),
			InsertOperation,
			func(operation Operation, key IKey, value *SimpleType) {
				triggerCount++
			},
		)
	}

	_, _ = Insert(db, NewSimpleType("t1", "t2", 1))

	// Assert
	if triggerCount != 5 {
		t.Errorf("AddBeforeTrigger failed: expected 5, got %d", triggerCount)
	}
}

func TestTrigger_CancelInsert(t *testing.T) {

	// Arrange
	db := prepareTestableDb()

	// Act
	_ = AddBeforeTrigger[SimpleType](
		db,
		"trigger",
		InsertOperation,
		func(operation Operation, key IKey, value *SimpleType) bool {
			return false
		},
	)
	objWrp, err := Insert(db, NewSimpleType("t1", "t2", 1))

	// Assert
	if !errors.Is(err, ErrCancelledByTrigger) {
		t.Errorf("Cancellation failed: expected %v, got %v", ErrCancelledByTrigger, err)

	}
	if objWrp.Key() != nil {
		t.Error("Cancellation failed: objWrp is not nil")
	}
}

// TODO add some test to ensure that parallelized access works as expected

//endregion
