package core_test

import (
	"bytes"
	. "github.com/Phosmachina/FluentKV/core"
	. "github.com/Phosmachina/FluentKV/helper"
	"testing"
)

type IKeyTester struct {
	key            IKey
	expectedKey    string
	expectedPrefix string
}

func NewIKeyTester(key IKey, expectedPrefix string, expectedKey string) IKeyTester {
	return IKeyTester{key: key, expectedKey: expectedKey, expectedPrefix: expectedPrefix}
}

func (i IKeyTester) TestPrefix(t *testing.T) {

	if i.key.Prefix() != i.expectedPrefix {
		t.Error()
	}
	if !bytes.Equal(i.key.RawPrefix(), []byte(i.expectedPrefix)) {
		t.Error()
	}
}

func (i IKeyTester) TestKey(t *testing.T) {

	if i.key.Key() != i.expectedKey {
		t.Error()
	}
	if !bytes.Equal(i.key.RawKey(), []byte(i.expectedKey)) {
		t.Error()
	}
}

type KeyWithIdTester struct {
	IKeyTester
	keyWithId  *KeyWithId
	expectedId string
}

func NewKeyWithIdTester(
	key *KeyWithId,
	expectedId string,
	expectedPrefix string,
	expectedKey string,
) KeyWithIdTester {
	return KeyWithIdTester{
		IKeyTester: NewIKeyTester(key, expectedPrefix, expectedKey),
		expectedId: expectedId,
	}
}

func (i KeyWithIdTester) TestId(t *testing.T) {

	keyWithId := i.key.(*KeyWithId)
	if keyWithId.Id() != i.expectedId {
		t.Error()
	}
}

type TableKeyTester struct {
	KeyWithIdTester
	key          *TableKey
	expectedName string
}

func NewTableKeyTester(
	key *TableKey,
	expectedName string,
	expectedId string,
	expectedPrefix string,
	expectedKey string,
) TableKeyTester {
	return TableKeyTester{
		key:          key,
		expectedName: expectedName,
		KeyWithIdTester: NewKeyWithIdTester(key.KeyWithId,
			expectedId,
			expectedPrefix,
			expectedKey),
	}
}

func (i TableKeyTester) TestName(t *testing.T) {
	if i.key.Name() != i.expectedName {
		t.Error()
	}
}

func arrangeTableKeyTest(expectedId string, expectedName string) (expectedPrefix, expectedKey string) {

	expectedPrefix = "tbl%" + expectedName
	if expectedName != "" {
		expectedPrefix += "_"
	}
	expectedKey = expectedPrefix + expectedId

	return expectedPrefix, expectedKey
}

func testNewTableKeyFromString(
	t *testing.T,
	expectedId string,
	expectedName string,
) {
	// Arrange
	_, expectedKey := arrangeTableKeyTest(expectedId, expectedName)

	// Act
	testedTableKey := NewTableKeyFromString(expectedKey)

	// Assert
	testNewTableKey(
		t,
		testedTableKey,
		expectedId,
		expectedName,
	)
}

func testNewTableKey(
	t *testing.T,
	testedTableKey *TableKey,
	expectedId string,
	expectedName string,
) {
	// Arrange
	expectedPrefix, expectedKey := arrangeTableKeyTest(expectedId, expectedName)
	tester := NewTableKeyTester(
		testedTableKey,
		expectedName,
		expectedId,
		expectedPrefix,
		expectedKey,
	)

	// Assert
	for _, test := range []func(*testing.T){
		tester.TestPrefix,
		tester.TestKey,
		tester.TestId,
		tester.TestName,
	} {
		t.Run(Name(test), func(t *testing.T) { test(t) })
	}
}

func TestNewProtoTableKey(t *testing.T) {
	testNewTableKey(t, NewProtoTableKey(), "", "")
}

func TestNewTableKeyFromString_WithoutNameAndId(t *testing.T) {
	testNewTableKeyFromString(t, "", "")
}

func TestNewTableKeyFromString_WithoutId(t *testing.T) {
	testNewTableKeyFromString(t, "", "TableName")
}

func TestNewTableKeyFromString_WithNameAndId(t *testing.T) {
	testNewTableKeyFromString(t, "Id", "TableName")
}

func TestNewTableKey_WithoutId(t *testing.T) {
	testNewTableKey(t, NewTableKey[SimpleType](), "", "SimpleType")
}

func TestNewTableKey_WithNameAndId(t *testing.T) {
	testerTableKey := NewTableKey[SimpleType]()
	testerTableKey.SetId("0")
	testNewTableKey(t, testerTableKey, "0", "SimpleType")
}

func TestEqualsTableKey(t *testing.T) {

	// Arrange
	type combination struct{ index1, index2 int }
	combinations := []combination{
		{0, 0}, {1, 1}, {2, 2},
		{0, 1}, {0, 2}, {2, 1}, {2, 0},
		{1, 2}, {2, 3},
	}
	keys := []*TableKey{
		NewTableKeyFromString("tbl%"),
		NewTableKeyFromString("tbl%TableName_"),
		NewTableKeyFromString("tbl%TableName_0"),
		NewTableKeyFromString("tbl%TableName_1"),
	}

	// Act & Assert
	for _, c := range combinations {
		equals := keys[c.index1].Equals(keys[c.index2])
		if (c.index1 == c.index2 && !equals) ||
			(equals && c.index1 != c.index2) {
			t.Error()
		}
	}
}
