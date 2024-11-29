package core

import "strings"

var (
	// PrefixTank prefix for a tank Id entry.
	PrefixTank = "tank" + PrefixDelimiter
	// PrefixTable prefix for a table entry.
	PrefixTable = "tbl" + PrefixDelimiter
	// PrefixLink prefix for a link declaration.
	PrefixLink = "lnk" + PrefixDelimiter

	PrefixDelimiter = "%"
	// IdDelimiter between the tableName and the key.
	IdDelimiter = "_"
	// LinkDelimiter between two keys for a link definition.
	LinkDelimiter = "@"

	// PrefixTankAvailableIds prefix for available ids.
	PrefixTankAvailableIds = PrefixTank + "avlbId" + IdDelimiter
	// PrefixTankUsedIds prefix for used ids.
	PrefixTankUsedIds = PrefixTank + "usedId" + IdDelimiter
)

type IKey interface {
	Prefix() string
	RawPrefix() []byte
	Key() string
	RawKey() []byte
}

func NewKeyFromString(key string) IKey {

	switch {
	case strings.HasPrefix(key, PrefixTankAvailableIds):
		return NewTankAvailableKeyFromString(key)
	case strings.HasPrefix(key, PrefixTankUsedIds):
		return NewTankUsedKeyFromString(key)
	case strings.HasPrefix(key, PrefixTable):
		return NewTableKeyFromString(key)
	case strings.HasPrefix(key, PrefixLink):
		return NewLinkKeyFromString(key)
	}

	return nil
}

type baseKey struct {
	IKey
}

func newBaseKey(key IKey) *baseKey {
	k := &baseKey{}
	k.IKey = key
	return k
}

func (b *baseKey) RawPrefix() []byte {
	return []byte(b.Prefix())
}

func (b *baseKey) RawKey() []byte {
	return []byte(b.Key())
}

type KeyWithId struct {
	*baseKey
	id string
}

func newKeyWithId(key IKey) *KeyWithId {
	k := &KeyWithId{}
	k.baseKey = newBaseKey(key)
	return k
}

func (k *KeyWithId) Id() string {
	return k.id
}

//region TankAvailableKey

type TankAvailableKey struct {
	*KeyWithId
	id string
}

func NewTankAvailableKeyFromString(key string) *TankAvailableKey {

	availableKey := &TankAvailableKey{}
	availableKey.KeyWithId = newKeyWithId(availableKey)

	id, found := strings.CutPrefix(key, PrefixTankAvailableIds)
	if !found || len(id) == 0 {
		return availableKey
	}

	availableKey.id = id

	return availableKey
}

func (t TankAvailableKey) Prefix() string {
	return PrefixTankAvailableIds
}

func (t TankAvailableKey) Key() string {
	return t.Prefix() + t.id
}

//endregion

//region TankUsedKey

type TankUsedKey struct {
	*KeyWithId
	id string
}

func NewTankUsedKeyFromString(key string) *TankUsedKey {

	tankUsedKey := &TankUsedKey{}
	tankUsedKey.KeyWithId = newKeyWithId(tankUsedKey)

	id, found := strings.CutPrefix(key, PrefixTankUsedIds)
	if !found || len(id) == 0 {
		return tankUsedKey
	}

	tankUsedKey.id = id

	return tankUsedKey
}

func (t TankUsedKey) Prefix() string {
	return PrefixTankUsedIds
}

func (t TankUsedKey) Key() string {
	return t.Prefix() + t.id
}

//endregion

//region TableKey

type TableKey struct {
	*KeyWithId
	name string
}

func NewProtoTableKey() *TableKey {
	key := &TableKey{}
	key.KeyWithId = newKeyWithId(key)
	return key
}

func NewTableKeyFromString(key string) *TableKey {

	tableKey := NewProtoTableKey()

	after, _ := strings.CutPrefix(key, PrefixTable)

	tableName, id, found := strings.Cut(after, IdDelimiter)
	tableKey.name = tableName
	if found {
		tableKey.id = id
	}

	return tableKey
}

func NewTableKeyFromObject(object IObject) *TableKey {
	key := NewProtoTableKey()
	key.name = object.TableName()
	return key
}

func NewTableKey[T IObject]() *TableKey {
	key := NewProtoTableKey()
	key.name = TableName[T]()
	return key
}

func (k *TableKey) Name() string {
	return k.name
}

func (k *TableKey) SetId(id string) *TableKey {
	k.id = id
	return k
}

func (k *TableKey) Prefix() string {

	if len(k.name) == 0 {
		return PrefixTable
	}

	return PrefixTable + k.name + IdDelimiter
}

func (k *TableKey) Key() string {
	return k.Prefix() + k.id
}

func (k *TableKey) Base() string {
	return k.name + IdDelimiter + k.id
}

func (k *TableKey) Equals(key *TableKey) bool {
	return k.name == key.name && k.id == key.id
}

//endregion

//region LinkKey

type LinkKey struct {
	*baseKey
	currentTableKey *TableKey
	targetTableKey  *TableKey
}

func NewProtoLinkKey() *LinkKey {
	k := &LinkKey{}
	k.baseKey = newBaseKey(k)
	return k
}

func NewLinkKeyFromString(key string) *LinkKey {

	base, _ := strings.CutPrefix(key, PrefixLink)

	links := strings.Split(base, LinkDelimiter)
	if len(links) != 2 {
		return NewProtoLinkKey()
	}

	return NewLinkKey(NewTableKeyFromString(links[0]), NewTableKeyFromString(links[1]))
}

func NewLinkKey(current *TableKey, target *TableKey) *LinkKey {

	key := NewProtoLinkKey()
	key.currentTableKey = current
	key.targetTableKey = target

	return key
}

func (l *LinkKey) CurrentTableKey() *TableKey {
	return l.currentTableKey
}

func (l *LinkKey) TargetTableKey() *TableKey {
	return l.targetTableKey
}

func (l *LinkKey) Prefix() string {
	return PrefixLink
}

func (l *LinkKey) Key() string {
	return l.Prefix() +
		l.currentTableKey.Base() +
		LinkDelimiter +
		l.targetTableKey.Base()
}

//endregion

// TODO Use Key* struct in place of static functions
// TODO (replace string by IKey in interface).
