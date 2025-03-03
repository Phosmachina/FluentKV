package core_test

import (
	"errors"
	. "github.com/Phosmachina/FluentKV/core"
	. "github.com/Phosmachina/FluentKV/helper"
	"sync"
	"testing"
)

//region Checker function

func checkInsert(
	t *testing.T,
	insert func(*KVStoreManager, *any) (*TableKey, error),
	insertFluent func(*KVStoreManager, *SimpleType) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := insertFluent != nil
	db := prepareTestableDb()
	expectedValue := NewSimpleType("t1", "t2", 1)
	expectedObject := any(expectedValue)
	expectedKey := NewTableKey[SimpleType]().SetId("0")

	// Act
	var err error
	var resultKey *TableKey
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = insertFluent(db, expectedValue)
	} else {
		resultKey, err = insert(db, &expectedObject)
	}

	// Assert
	if err != nil {
		t.Errorf("insert failed: expected %v, got %v", nil, err)
	}
	if !db.Exist(expectedKey) {
		t.Error("Expecting existing key after insert")
	}

	if isFluentTest {
		checkObjectWrapper(t, resultObjWrp, expectedKey, &expectedObject)
	} else if resultKey == nil || !resultKey.Equals(expectedKey) {
		t.Errorf("insert failed: expected %v, got %v", expectedKey, resultKey)
	}
}

func CheckGetValidId(
	t *testing.T,
	get func(*KVStoreManager, *TableKey) (*any, error),
	getFluent func(*KVStoreManager, string) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := getFluent != nil
	db := prepareTestableDb()
	expectedObject := any(NewSimpleType("t1", "t2", 1))
	expectedKey, _ := db.Insert(&expectedObject)

	// Act
	var err error
	var resultObject *any
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = getFluent(db, expectedKey.Id())
	} else {
		resultObject, err = get(db, expectedKey)
	}

	// Assert
	if err != nil {
		t.Errorf("Get failed: expected %v, got %v", nil, err)
	}

	if isFluentTest {
		checkObjectWrapper(t, resultObjWrp, expectedKey, &expectedObject)
	} else if !Equals(*resultObject, expectedObject) {
		t.Errorf("Get failed: expected %v, got %v", expectedObject, resultObject)
	}
}

func CheckGetInvalidId(
	t *testing.T,
	get func(*KVStoreManager, *TableKey) (*any, error),
	getFluent func(*KVStoreManager, string) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := getFluent != nil
	db := prepareTestableDb()
	expectedKey := NewTableKey[SimpleType]().SetId("0")

	// Act
	var err error
	var resultObject *any
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = getFluent(db, expectedKey.Id())
	} else {
		resultObject, err = get(db, expectedKey)
	}

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("Get failed: expected %v, got %v", ErrInvalidId, err)
	}

	if isFluentTest {
		if !resultObjWrp.IsEmpty() {
			t.Errorf("Get failed: expected %v, got %v", nil, resultObjWrp)
		}
	} else {
		if resultObject != nil {
			t.Errorf("Get failed: expected %v, got %v", nil, resultObject)
		}
	}
}

func CheckSetValidId(
	t *testing.T,
	set func(*KVStoreManager, *TableKey, *any) error,
	setFluent func(*KVStoreManager, string, *SimpleType) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := setFluent != nil
	db := prepareTestableDb()
	initialValue := NewSimpleType("t1", "t2", 1)
	initialValueAsAny := any(initialValue)
	expectedKey, _ := db.Insert(&initialValueAsAny)
	expectedValue := NewSimpleType("t1", "t2", 2)
	expectedValueAsAny := any(expectedValue)

	// Act
	var err error
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = setFluent(db, expectedKey.Id(), expectedValue)
	} else {
		err = set(db, expectedKey, &expectedValueAsAny)
	}

	// Assert
	if err != nil {
		t.Errorf("set failed: expected %v, got %v", nil, err)
	}

	if isFluentTest {
		checkObjectWrapper(t, resultObjWrp, expectedKey, &expectedValueAsAny)
	} else {
		resultObject, _ := db.Get(expectedKey)
		if !Equals(*resultObject, expectedValueAsAny) {
			t.Errorf("set failed: expected %v, got %v", expectedValueAsAny, resultObject)
		}
	}
}

func CheckSetInvalidId(
	t *testing.T,
	set func(*KVStoreManager, *TableKey, *any) error,
	setFluent func(*KVStoreManager, string, *SimpleType) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := setFluent != nil
	db := prepareTestableDb()
	expectedKey := NewTableKey[SimpleType]().SetId("0")

	// Act
	var err error
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = setFluent(db, expectedKey.Id(), NewSimpleType("t1", "t2", 1))
	} else {
		object := any(NewSimpleType("t1", "t2", 1))
		err = set(db, expectedKey, &object)
	}

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("set failed: expected %v, got %v", ErrInvalidId, err)
	}

	if isFluentTest {
		if !resultObjWrp.IsEmpty() {
			t.Errorf("set failed: expected %v, got %v", nil, resultObjWrp)
		}
	}
}

func CheckUpdate(
	t *testing.T,
	update func(*KVStoreManager, *TableKey, func(*any) *any) (*any, error),
	updateFluent func(*KVStoreManager, string, func(*SimpleType)) (KVWrapper[SimpleType], error),
) {
	// Arrange
	isFluentTest := updateFluent != nil
	db := prepareTestableDb()
	initialObjWrp, _ := Insert(db, NewSimpleType("t1", "t2", 0))
	expectedValue := 2

	// Act
	var err error
	var resultObject *any
	var resultObjWrp KVWrapper[SimpleType]

	if isFluentTest {
		resultObjWrp, err = updateFluent(db, initialObjWrp.Key().Id(), func(value *SimpleType) {
			value.Val = expectedValue
		})
	} else {
		resultObject, err = update(db, initialObjWrp.Key(), func(value *any) *any {
			updatedAsSimpleType := (*value).(SimpleType)
			updatedAsSimpleType.Val = expectedValue
			updatedAsAny := any(updatedAsSimpleType)
			return &updatedAsAny
		})
	}

	// Assert
	if err != nil {
		t.Errorf("Update failed: expected %v, got %v", nil, err)
	}

	expectedValueAsAny := any(NewSimpleType("t1", "t2", expectedValue))
	if isFluentTest {
		checkObjectWrapper(t, resultObjWrp, initialObjWrp.Key(), &expectedValueAsAny)
	} else {
		if !Equals(*resultObject, expectedValueAsAny) {
			t.Errorf(
				"Update failed: expected %v, got %v",
				expectedValueAsAny,
				(*resultObject).(SimpleType))
		}
	}

	resultObjWrp, _ = Get[SimpleType](db, initialObjWrp.Key().Id())
	if resultObjWrp.Value().Val != expectedValue {
		t.Errorf(
			"Update failed: expected %v, got %v",
			expectedValue,
			resultObjWrp.Value().Val,
		)
	}
}

// CheckDeleteValidId verifies deleting a valid record removes it from the store.
func CheckDeleteValidId(
	t *testing.T,
	deleteNative func(*KVStoreManager, *TableKey) error,
	deleteFluent func(*KVStoreManager, string) error,
) {
	isFluentTest := deleteFluent != nil

	// Arrange
	db := prepareTestableDb()
	obj := any(NewSimpleType("t1", "t2", 3))
	key, _ := db.Insert(&obj)

	// Act
	var err error
	if isFluentTest {
		err = deleteFluent(db, key.Id())
	} else {
		err = deleteNative(db, key)
	}

	// Assert
	if err != nil {
		t.Errorf("Delete failed: expected %v, got %v", nil, err)
	}
	if db.Exist(key) {
		t.Errorf("Delete failed: key %v still exists after deletion", key)
	}
}

// CheckDeleteInvalidId verifies deleting an invalid record returns ErrInvalidId.
func CheckDeleteInvalidId(
	t *testing.T,
	deleteNative func(*KVStoreManager, *TableKey) error,
	deleteFluent func(*KVStoreManager, string) error,
) {
	isFluentTest := deleteFluent != nil

	// Arrange
	db := prepareTestableDb()
	invalidKey := NewTableKey[SimpleType]().SetId("0")

	// Act
	var err error
	if isFluentTest {
		err = deleteFluent(db, invalidKey.Id())
	} else {
		err = deleteNative(db, invalidKey)
	}

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("Delete with invalid ID failed: expected %v, got %v", ErrInvalidId, err)
	}
}

// CheckDeepDelete verifies deep deletion of a valid record.
func CheckDeepDelete(
	t *testing.T,
	deepDeleteNative func(*KVStoreManager, *TableKey) error,
	deepDeleteFluent func(*KVStoreManager, string) error,
) {
	isFluentTest := deepDeleteFluent != nil

	// Arrange
	db := prepareTestableDb()
	obj := any(NewSimpleType("tDeep", "del", 9))
	key, _ := db.Insert(&obj)

	// Act
	var err error
	if isFluentTest {
		err = deepDeleteFluent(db, key.Id())
	} else {
		err = deepDeleteNative(db, key)
	}

	// Assert
	if err != nil {
		t.Errorf("DeepDelete failed: expected %v, got %v", nil, err)
	}
	if db.Exist(key) {
		t.Errorf("DeepDelete failed: key %v still exists after deep deletion", key)
	}
}

// CheckExist verifies that a known entry returns true, and unknown entry returns false.
func CheckExist(
	t *testing.T,
	existNative func(*KVStoreManager, *TableKey) bool,
	existFluent func(*KVStoreManager, string) bool,
) {
	isFluentTest := existFluent != nil

	// Arrange
	db := prepareTestableDb()
	obj := any(NewSimpleType("tExist", "test", 10))
	key, _ := db.Insert(&obj)
	invalidKey := NewTableKey[SimpleType]().SetId("42")

	// Act & Assert: valid key
	var result bool
	if isFluentTest {
		result = existFluent(db, key.Id())
	} else {
		result = existNative(db, key)
	}
	if !result {
		t.Errorf("Exist check failed: expected true, got false for key %v", key)
	}

	// Act & Assert: invalid key
	if isFluentTest {
		result = existFluent(db, invalidKey.Id())
	} else {
		result = existNative(db, invalidKey)
	}
	if result {
		t.Errorf("Exist check failed: expected false, got true for invalid key %v", invalidKey)
	}
}

// CheckCount verifies that Count properly reflects the number of inserted items.
func CheckCount(
	t *testing.T,
	countNative func(*KVStoreManager) int,
	countFluent func(*KVStoreManager) int,
) {
	isFluentTest := countFluent != nil

	// Arrange
	db := prepareTestableDb()

	// Act: count on empty DB
	var total int
	if isFluentTest {
		total = countFluent(db)
	} else {
		total = countNative(db)
	}

	// Assert: should be zero
	if total != 0 {
		t.Errorf("Count failed on empty DB: expected 0, got %v", total)
	}

	// Arrange: Insert some entries
	for i := 0; i < 3; i++ {
		val := any(NewSimpleType("count", "test", i))
		_, _ = db.Insert(&val)
	}

	// Act: count again
	if isFluentTest {
		total = countFluent(db)
	} else {
		total = countNative(db)
	}

	// Assert: should be 3
	if total != 3 {
		t.Errorf("Count failed: expected 3, got %v", total)
	}
}

func CheckForEach(
	t *testing.T,
	forEachNative func(*KVStoreManager, func(*TableKey, *any)),
	forEachFluent func(*KVStoreManager, func(IKey, *SimpleType)),
) {
	isFluentTest := forEachFluent != nil

	// Arrange
	db := prepareTestableDb()
	for i := 1; i <= 3; i++ {
		val := any(NewSimpleType("forEach", "test", i))
		_, _ = db.Insert(&val)
	}

	visited := make(map[int]bool)
	actionNative := func(_ *TableKey, obj *any) {
		simple := (*obj).(SimpleType)
		visited[simple.Val] = true
	}
	actionFluent := func(_ IKey, simple *SimpleType) {
		visited[simple.Val] = true
	}

	// Act
	if isFluentTest {
		forEachFluent(db, actionFluent)
	} else {
		forEachNative(db, actionNative)
	}

	// Assert
	for i := 1; i <= 3; i++ {
		if !visited[i] {
			t.Errorf("ForEach failed: expected to visit Val=%d, but did not", i)
		}
	}
}

func CheckFindFirst(
	t *testing.T,
	findFirstNative func(*KVStoreManager, func(*TableKey, *any) bool) (*TableKey, *any),
	findFirstFluent func(*KVStoreManager, func(*TableKey, *SimpleType) bool) (KVWrapper[SimpleType],
		error),
) {
	isFluentTest := findFirstFluent != nil

	// Arrange
	db := prepareTestableDb()

	expectedObject := NewSimpleType("t1", "t2", 42)
	var possibleExpectedKeys []*TableKey
	for i := 0; i < 3; i++ {
		_, _ = Insert(db, NewSimpleType("t1", "t2", i))
	}
	for i := 0; i < 3; i++ {
		objWrp, _ := Insert(db, expectedObject)
		possibleExpectedKeys = append(possibleExpectedKeys, objWrp.Key())
	}

	// Act
	var firstMatch KVWrapper[SimpleType]

	if isFluentTest {
		firstMatch, _ = findFirstFluent(db, func(_ *TableKey, value *SimpleType) bool {
			return value.Val == 42
		})
	} else {
		firstKey, firstObject := findFirstNative(db, func(_ *TableKey, obj *any) bool {
			return (*obj).(SimpleType).Val == 42
		})
		firstSimpleType := (*firstObject).(SimpleType)
		firstMatch = NewKVWrapper[SimpleType](db, firstKey, &firstSimpleType)
	}

	// Assert
	found := false
	for _, key := range possibleExpectedKeys {
		if key.Equals(firstMatch.Key()) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("FindFirst failed:  got %v", firstMatch.Key())
	}
	if !Equals(*firstMatch.Value(), *expectedObject) {
		t.Errorf("FindFirst failed: expected %v, got %v", expectedObject, firstMatch.Value())
	}
}

func CheckFindAll(
	t *testing.T,
	findAllNative func(*KVStoreManager, func(*TableKey, *any) bool) ([]*TableKey, []*any),
	findAllFluent func(*KVStoreManager, func(*TableKey, *SimpleType) bool) []KVWrapper[SimpleType],
) {
	isFluentTest := findAllFluent != nil

	// Arrange
	db := prepareTestableDb()
	expectedObject := NewSimpleType("t1", "t2", 42)
	var expectedResults []KVWrapper[SimpleType]
	for i := 0; i < 3; i++ {
		_, _ = Insert(db, NewSimpleType("t1", "t2", i))

	}
	for i := 0; i < 3; i++ {
		objWrp, _ := Insert(db, expectedObject)
		expectedResults = append(expectedResults, objWrp)
	}

	// Act
	var allResults []KVWrapper[SimpleType]

	if isFluentTest {
		allResults = findAllFluent(db, func(_ *TableKey, value *SimpleType) bool {
			return value.Val == 42
		})
	} else {
		allKeys, allObjects := findAllNative(
			db,
			func(_ *TableKey, value *any) bool {
				return (*value).(SimpleType).Val == 42
			},
		)
		for i := range allKeys {
			simpleType := (*allObjects[i]).(SimpleType)
			allResults = append(
				allResults,
				NewKVWrapper[SimpleType](db, allKeys[i], &simpleType),
			)
		}
	}

	// Assert
	if len(allResults) != len(expectedResults) {
		t.Errorf("FindAll failed: expected %v, got %v", len(expectedResults), len(allResults))
	}

	for _, expectedResult := range expectedResults {
		if !RemoveWithPredicate(&allResults, expectedResult, func(o1 KVWrapper[SimpleType],
			o2 KVWrapper[SimpleType]) bool {
			return Equals(o1.Value(), o2.Value()) && o1.Key().Equals(o2.Key())
		}) {
			t.Errorf("FindAll failed: expected %v", expectedResult)
		}
	}
}

// CheckSetWrp verifies setting an existing wrapper updates the store.
func CheckSetWrp(
	t *testing.T,
	setWrpNative func(*KVStoreManager, KVWrapper[SimpleType]) error,
	setWrpFluent func(*KVStoreManager, KVWrapper[SimpleType]) (KVWrapper[SimpleType], error),
) {
	isFluentTest := setWrpFluent != nil
	db := prepareTestableDb()
	objWrp, _ := Insert(db, NewSimpleType("wrp", "test", 5))

	// Modify
	objWrp.Value().Val = 10

	// Act
	var err error
	var updated KVWrapper[SimpleType]
	if isFluentTest {
		updated, err = setWrpFluent(db, objWrp)
	} else {
		err = setWrpNative(db, objWrp)
	}

	// Assert
	if err != nil {
		t.Errorf("SetWrp failed: expected %v, got %v", nil, err)
	}
	checkVal := 10
	var readBack KVWrapper[SimpleType]
	readBack, _ = Get[SimpleType](db, objWrp.Key().Id())

	if isFluentTest {
		if updated.IsEmpty() || updated.Value().Val != checkVal {
			t.Errorf("SetWrp failed: expected Val=%v, got %v", checkVal, updated.Value().Val)
		}
	}
	if readBack.Value().Val != checkVal {
		t.Errorf("SetWrp failed: expected Val=%v, read back %v", checkVal, readBack.Value().Val)
	}
}

// CheckDeleteWrp verifies deleting a valid wrapper removes it from the store.
func CheckDeleteWrp(
	t *testing.T,
	deleteWrpNative func(*KVStoreManager, KVWrapper[SimpleType]) error,
	deleteWrpFluent func(*KVStoreManager, KVWrapper[SimpleType]) error,
) {
	// Arrange
	db := prepareTestableDb()
	objWrp, _ := Insert(db, NewSimpleType("delWrp", "test", 7))

	// Act
	var err error
	if deleteWrpFluent != nil {
		err = deleteWrpFluent(db, objWrp)
	} else if deleteWrpNative != nil {
		err = deleteWrpNative(db, objWrp)
	} else {
		t.Error("No deleteWrp methods provided")
		return
	}

	// Assert
	if err != nil {
		t.Errorf("DeleteWrp failed: expected %v, got %v", nil, err)
	}
	if db.Exist(objWrp.Key()) {
		t.Errorf("DeleteWrp failed: key %v still exists after delete", objWrp.Key())
	}
}

// CheckDeepDeleteWrp verifies deep deletion for wrappers.
func CheckDeepDeleteWrp(
	t *testing.T,
	deepDeleteWrpNative func(*KVStoreManager, KVWrapper[SimpleType]) error,
	deepDeleteWrpFluent func(*KVStoreManager, KVWrapper[SimpleType]) error,
) {
	// Arrange
	db := prepareTestableDb()
	objWrp, _ := Insert(db, NewSimpleType("deepWrp", "test", 11))

	// Act
	var err error
	if deepDeleteWrpFluent != nil {
		err = deepDeleteWrpFluent(db, objWrp)
	} else if deepDeleteWrpNative != nil {
		err = deepDeleteWrpNative(db, objWrp)
	} else {
		t.Error("No deepDeleteWrp methods provided")
		return
	}

	// Assert
	if err != nil {
		t.Errorf("DeepDeleteWrp failed: expected %v, got %v", nil, err)
	}
	if db.Exist(objWrp.Key()) {
		t.Errorf("DeepDeleteWrp failed: key %v still exists after deep delete", objWrp.Key())
	}
}

// CheckExistWrp verifies that a known wrapper returns true, and an unknown wrapper returns false.
func CheckExistWrp(
	t *testing.T,
	existWrpNative func(*KVStoreManager, KVWrapper[SimpleType]) bool,
	existWrpFluent func(*KVStoreManager, KVWrapper[SimpleType]) bool,
) {
	// Arrange
	db := prepareTestableDb()
	wrp, _ := Insert(db, NewSimpleType("existWrp", "test", 15))

	// Act: valid wrapper
	var result bool
	if existWrpFluent != nil {
		result = existWrpFluent(db, wrp)
	} else if existWrpNative != nil {
		result = existWrpNative(db, wrp)
	} else {
		t.Error("No existWrp methods provided")
		return
	}

	// Assert
	if !result {
		t.Error("ExistWrp failed: expected true for existing wrapper, got false")
	}

	// Arrange: an empty wrapper with invalid ID
	bogusKey := NewTableKey[SimpleType]().SetId("42")
	bogusWrp := NewKVWrapper[SimpleType](db, bogusKey, nil)

	// Act: invalid wrapper
	if existWrpFluent != nil {
		result = existWrpFluent(db, bogusWrp)
	} else {
		result = existWrpNative(db, bogusWrp)
	}

	// Assert
	if result {
		t.Error("ExistWrp failed: expected false for invalid wrapper, got true")
	}
}

//endregion

func TestInsert(t *testing.T) {
	checkInsert(t, func(db *KVStoreManager, object *any) (*TableKey, error) {
		return db.Insert(object)
	}, nil)
}

func TestDelete_WithoutLink_ValidId(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	objWrp1, _ := Insert(db, NewSimpleType("t1", "t2", 1))
	objWrp2, _ := Insert(db, NewSimpleType("t1", "t2", 2))

	// Act
	err := db.Delete(objWrp1.Key())

	// Assert
	if err != nil {
		t.Errorf("Delete failed: expected %v, got %v", nil, err)
	}
	if db.Exist(objWrp1.Key()) {
		t.Error("Delete failed: object still exists")
	}
	if !db.Exist(objWrp2.Key()) {
		t.Error("Delete operation remove unexpected object")
	}
}

func TestDelete_WithoutLink_InvalidId(t *testing.T) {

	// Arrange
	db := prepareTestableDb()
	key := NewTableKey[SimpleType]().SetId("0")

	// Act
	err := db.Delete(key)

	// Assert
	if !errors.Is(err, ErrInvalidId) {
		t.Errorf("Delete failed: expected %v, got %v", ErrInvalidId, err)
	}
}

func TestGet_ValidId(t *testing.T) {
	CheckGetValidId(t, func(db *KVStoreManager, key *TableKey) (*any, error) {
		return db.Get(key)
	}, nil)
}

func TestGet_InvalidId(t *testing.T) {
	CheckGetInvalidId(t, func(db *KVStoreManager, key *TableKey) (*any, error) {
		return db.Get(key)
	}, nil)
}

func TestSet_ValidId(t *testing.T) {
	CheckSetValidId(t, func(db *KVStoreManager, key *TableKey, value *any) error {
		return db.Set(key, value)
	}, nil)
}

func TestSet_InvalidId(t *testing.T) {
	CheckSetInvalidId(t, func(db *KVStoreManager, key *TableKey, value *any) error {
		return db.Set(key, value)
	}, nil)
}

func TestAutoKeying(t *testing.T) {

	// Arrange
	AutoIdBuffer = 15
	db := prepareTestableDb()
	expectedIds1 := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	expectedIds2 := []string{"10", "11", "12", "13", "14", "0", "1", "2", "3", "4", "15", "16", "17", "18", "19"}
	expectedIds3 := []string{"10", "12", "14", "16", "18", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29"}

	// Act
	var resultIds1 []string
	for i := 0; i < len(expectedIds1); i++ {
		objWrp, _ := Insert(db, NewSimpleType("t1", "t2", 0))
		resultIds1 = append(resultIds1, objWrp.Key().Id())
	}

	for _, id := range []string{"0", "1", "2", "3", "4"} { // Delete some objects...
		if db.Delete(NewTableKey[SimpleType]().SetId(id)) != nil {
			t.Error()
		}
	}

	var resultIds2 []string
	for i := 0; i < len(expectedIds2); i++ {
		objWrp, _ := Insert(db, NewSimpleType("t1", "t2", 0))
		resultIds2 = append(resultIds2, objWrp.Key().Id())
	}

	for _, id := range []string{"10", "12", "14", "16", "18"} { // Delete some objects...
		if db.Delete(NewTableKey[SimpleType]().SetId(id)) != nil {
			t.Error()
		}
	}

	var resultIds3 []string
	for i := 0; i < len(expectedIds3); i++ {
		objWrp, _ := Insert(db, NewSimpleType("t1", "t2", 0))
		resultIds3 = append(resultIds3, objWrp.Key().Id())
	}

	// Assert
	for _, id := range expectedIds1 {
		if !Remove(&resultIds1, id) {
			t.Errorf("AutoKeying failed: expected %v not found in result ids.", id)
		}
	}
	for _, id := range expectedIds2 {
		if !Remove(&resultIds2, id) {
			t.Errorf("AutoKeying failed: expected %v not found in result ids.", id)
		}
	}
	for _, id := range expectedIds3 {
		if !Remove(&resultIds3, id) {
			t.Errorf("AutoKeying failed: expected %v not found in result ids.", id)
		}
	}
}

func TestUpdate(t *testing.T) {
	CheckUpdate(
		t, func(
			db *KVStoreManager,
			key *TableKey,
			editor func(*any) *any,
		) (*any, error) {
			return db.Update(key, editor)
		}, nil)
}

func TestDeleteValidId(t *testing.T) {
	CheckDeleteValidId(
		t,
		func(db *KVStoreManager, key *TableKey) error {
			return db.Delete(key)
		},
		nil,
	)
}

func TestDeleteInvalidId(t *testing.T) {
	CheckDeleteInvalidId(
		t,
		func(db *KVStoreManager, key *TableKey) error {
			return db.Delete(key)
		},
		nil,
	)
}

func TestDeepDelete(t *testing.T) {
	CheckDeepDelete(
		t,
		func(db *KVStoreManager, key *TableKey) error {
			return db.DeepDelete(key)
		},
		nil,
	)
}

func TestExist(t *testing.T) {
	CheckExist(
		t,
		func(db *KVStoreManager, key *TableKey) bool {
			return db.Exist(key)
		},
		nil,
	)
}

func TestCount(t *testing.T) {
	CheckCount(
		t,
		func(db *KVStoreManager) int {
			return db.Count(NewTableKey[SimpleType]())
		},
		nil,
	)
}

func TestForEach(t *testing.T) {
	CheckForEach(
		t,
		func(db *KVStoreManager, action func(*TableKey, *any)) {
			db.Foreach(NewTableKey[SimpleType](), action)
		},
		nil,
	)
}

func TestFindFirst(t *testing.T) {
	CheckFindFirst(
		t,
		func(db *KVStoreManager, predicate func(*TableKey, *any) bool) (*TableKey, *any) {
			return db.FindFirst(NewTableKey[SimpleType](), predicate)
		},
		nil,
	)
}

func TestFindAll(t *testing.T) {
	CheckFindAll(
		t,
		func(db *KVStoreManager, predicate func(*TableKey, *any) bool) ([]*TableKey, []*any) {
			return db.FindAll(NewTableKey[SimpleType](), predicate)
		},
		nil,
	)
}

func FuzzCount(f *testing.F) {

	f.Add(0)
	f.Add(1)
	f.Add(5)

	f.Fuzz(func(t *testing.T, initialObjects int) {
		// Arrange
		db := prepareTestableDb()

		for i := 0; i < initialObjects; i++ {
			_, _ = Insert(db, NewSimpleType("t1", "t2", i))
		}

		// Act
		count := db.Count(NewTableKey[SimpleType]())

		// Assert
		if count != initialObjects {
			t.Errorf("Count failed: expected %v, got %v", initialObjects, count)
		}
	})
}

func TestForeach(t *testing.T) {

	// Arrange
	db := prepareTestableDb()

	expectedResults := make(map[string]*any)
	for i := 0; i < 5; i++ {
		objWrp, _ := Insert(db, NewSimpleType("t1", "t2", i))
		valueAsAny := any(*objWrp.Value())
		expectedResults[objWrp.Key().Id()] = &valueAsAny
	}

	// Act
	forEachValues := make(map[string]*any)
	m := sync.Mutex{}
	db.Foreach(NewProtoTableKey(), func(tableKey *TableKey, value *any) {
		m.Lock()
		forEachValues[tableKey.Id()] = value
		m.Unlock()
	})

	// Assert
	if len(expectedResults) != len(forEachValues) {
		t.Errorf("Foreach failed: expected %v, got %v", len(expectedResults), len(forEachValues))
	}
	for id, expectedObject := range expectedResults {
		object := forEachValues[id]
		if !Equals(*object, *expectedObject) {
			t.Errorf("Foreach failed: expected %v, got %v", expectedObject, object)
		}
	}
}
