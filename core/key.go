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
	case strings.HasPrefix(PrefixTankAvailableIds, key):
		return NewTankAvailableKeyFromString(key)
	case strings.HasPrefix(PrefixTankUsedIds, key):
		return NewTankUsedKeyFromString(key)
	case strings.HasPrefix(PrefixTable, key):
		return NewTableKeyFromString(key)
	case strings.HasPrefix(PrefixLink, key):
		return NewLinkKeyFromString(key)
	}

	return nil
}

type baseKey struct {
	IKey
}

func (b baseKey) RawPrefix() []byte {
	return []byte(b.Prefix())
}

func (b baseKey) RawKey() []byte {
	return []byte(b.Key())
}

type keyWithId struct {
	baseKey
	id string
}

func (k keyWithId) Id() string {
	return k.id
}

//region TankAvailableKey

type TankAvailableKey struct {
	keyWithId
	id string
}

func NewTankAvailableKeyFromString(key string) TankAvailableKey {

	availableKey := TankAvailableKey{}

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
	keyWithId
	id string
}

func NewTankUsedKeyFromString(key string) TankUsedKey {

	tankUsedKey := TankUsedKey{}

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
	keyWithId
	name string
}

func NewTableKeyFromString(key string) TableKey {

	tableKey := TableKey{}
	tableKey.IKey = tableKey

	after, _ := strings.CutPrefix(key, PrefixTable)

	split := strings.Split(after, IdDelimiter)
	if len(split) != 2 {
		return tableKey
	}
	tableKey.name = split[0]
	tableKey.id = split[1]

	return tableKey
}

func NewTableKeyFromObject(object IObject) TableKey {
	return TableKey{name: object.TableName()}
}

func NewTableKey[T IObject]() TableKey {
	return TableKey{name: TableName[T]()}
}

func (k TableKey) Name() string {
	return k.name
}

func (k TableKey) SetId(id string) TableKey {
	k.id = id
	return k
}

func (k TableKey) Prefix() string {

	if len(k.name) == 0 {
		return PrefixTable
	}

	return PrefixTable + k.name + IdDelimiter
}

func (k TableKey) Key() string {
	return k.Prefix() + k.id
}

func (k TableKey) Base() string {
	return k.name + IdDelimiter + k.id
}

func (k TableKey) Equals(key TableKey) bool {
	return k.name == key.name && k.id == key.id
}

//endregion

//region LinkKey

type LinkKey struct {
	baseKey
	currentTableKey TableKey
	targetTableKey  TableKey
}

func NewLinkKeyFromString(key string) LinkKey {

	base, _ := strings.CutPrefix(key, PrefixLink)

	links := strings.Split(base, LinkDelimiter)
	if len(links) != 2 {
		return LinkKey{}
	}

	return NewLinkKey(NewTableKeyFromString(links[0]), NewTableKeyFromString(links[1]))
}

func NewLinkKey(current TableKey, target TableKey) LinkKey {
	return LinkKey{
		currentTableKey: current,
		targetTableKey:  target,
	}
}

func (l LinkKey) CurrentTableKey() TableKey {
	return l.currentTableKey
}

func (l LinkKey) TargetTableKey() TableKey {
	return l.targetTableKey
}

func (l LinkKey) Prefix() string {
	return PrefixLink
}

func (l LinkKey) Key() string {
	return l.Prefix() +
		l.currentTableKey.Base() +
		LinkDelimiter +
		l.targetTableKey.Base()
}

//endregion

// TODO Use Key* struct in place of static functions
// TODO (replace string by IKey in interface).
