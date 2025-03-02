package core

import (
	"github.com/Phosmachina/FluentKV/helper"
	"strings"
)

// The following constants define core elements for prefixes and delimiters.
// They encapsulate data domain concepts such as "tank", "table", or "link."
// Adjusting these constants allows you to customize key representation.
var (
	// PrefixTank indicates a domain concept related to "tank" entries.
	PrefixTank = "tank" + PrefixDelimiter

	// PrefixTable indicates a domain concept related to "table" entries.
	PrefixTable = "tbl" + PrefixDelimiter

	// PrefixLink denotes a relationship or link between two entities.
	PrefixLink = "lnk" + PrefixDelimiter

	// PrefixDelimiter acts as a general separator for domain-related prefixes.
	PrefixDelimiter = "%"

	// IdDelimiter separates a table name and the identification portion within a key.
	IdDelimiter = "_"

	// LinkDelimiter separates two references for a link definition.
	LinkDelimiter = "@"

	// PrefixTankAvailableIds marks entries for available IDs within the "tank" domain concept.
	PrefixTankAvailableIds = PrefixTank + "avlbId" + IdDelimiter

	// PrefixTankUsedIds marks entries for used IDs within the "tank" domain concept.
	PrefixTankUsedIds = PrefixTank + "usedId" + IdDelimiter
)

// IKey describes the capabilities required for a structured key,
// supporting both string and byte-oriented retrieval of prefix and key content.
type IKey interface {
	Prefix() string
	RawPrefix() []byte
	Key() string
	RawKey() []byte
}

// NewKeyFromString inspects a plain string and produces an IKey that
// aligns with one of the known domain concepts (tank availability, tank usage,
// table reference, or link reference).
// If the input key does not match any expected prefix, this function returns nil.
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

// baseKey provides underlying shared functionality for deriving prefix and
// complete key content as byte slices.
// It can be embedded into higher-level key types.
type baseKey struct {
	IKey
}

// newBaseKey embeds an existing IKey into a baseKey, centralizing raw access methods.
func newBaseKey(key IKey) *baseKey {
	k := &baseKey{}
	k.IKey = key
	return k
}

// RawPrefix returns the prefix portion in byte form.
// This aids in low-level handling or comparison of prefix data.
func (b *baseKey) RawPrefix() []byte {
	return []byte(b.Prefix())
}

// RawKey returns the full key in byte form.
// This provides a convenient approach for interacting with byte-based APIs.
func (b *baseKey) RawKey() []byte {
	return []byte(b.Key())
}

// KeyWithId extends a base key with an identifier component. It is
// useful when working with entities that must track a unique ID.
type KeyWithId struct {
	*baseKey
	id string
}

// newKeyWithId creates a KeyWithId by wrapping any IKey with an additional
// identifier field. This allows for flexible composition of key structures.
func newKeyWithId(key IKey) *KeyWithId {
	k := &KeyWithId{}
	k.baseKey = newBaseKey(key)
	return k
}

// Id retrieves the unique identifier associated with this KeyWithId.
func (k *KeyWithId) Id() string {
	return k.id
}

//region TankAvailableKey

// TankAvailableKey addresses the concept of "available IDs" in a "tank" domain.
// It embeds the idea of an identifier within a specialized structure.
type TankAvailableKey struct {
	*KeyWithId
}

// NewTankAvailableKeyFromString constructs a key object, extracting the ID
// from a raw string that reflects the "available ID" domain concept.
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

// Prefix provides a conceptual grouping indicator for this type of key;
// it references the underlying prefix constants to align with the data domain.
func (t TankAvailableKey) Prefix() string {
	return PrefixTankAvailableIds
}

// Key combines the conceptual prefix with the identifier to produce the full representation.
func (t TankAvailableKey) Key() string {
	return t.Prefix() + t.id
}

//endregion

//region TankUsedKey

// TankUsedKey addresses the concept of "used IDs" in a "tank" domain, embedding
// an identifier to track usage within the data store.
type TankUsedKey struct {
	*KeyWithId
}

// NewTankUsedKeyFromString creates a specialized key object from a string that indicates
// the "used ID" domain concept, extracting the embedded identifier.
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

// Prefix represents a domain-specific marker for "used ID" content,
// derived from customizable constants.
func (t TankUsedKey) Prefix() string {
	return PrefixTankUsedIds
}

// Key merges the domain-specific prefix with the unique identifier to
// form a complete key representation in the "used ID" context.
func (t TankUsedKey) Key() string {
	return t.Prefix() + t.id
}

//endregion

//region TableKey

// TableKey represents a higher-level entity in the data store, capturing both a
// table name and an identifier. This allows flexible use within different table-based domains.
type TableKey struct {
	*KeyWithId
	name string
}

// NewProtoTableKey creates a basic instance of TableKey that can be customized
// with a name and optional ID. This approach prevents nil references by returning
// a ready-to-use structure.
func NewProtoTableKey() *TableKey {
	key := &TableKey{}
	key.KeyWithId = newKeyWithId(key)
	return key
}

// NewTableKeyFromString parses a raw string to populate a TableKey with its identified
// domain name and ID component. If the domain name or ID is missing, they remain unset.
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

// NewTableKeyFromObject constructs a TableKey by inspecting the structure name of a given object.
// This approach loosely maps an object type to a table domain name.
func NewTableKeyFromObject(value any) *TableKey {
	key := NewProtoTableKey()
	key.name = helper.StructName(value)
	return key
}

// NewTableKey generically instantiates a TableKey by inferring the
// table name from a given type, thereby avoiding manual name assignments.
func NewTableKey[T any]() *TableKey {
	key := NewProtoTableKey()
	key.name = TableName[T]()
	return key
}

// Name returns the stored domain name, generally intended to represent a table name.
func (k *TableKey) Name() string {
	return k.name
}

// SetId assigns an identifier to a TableKey, enabling customization for
// specific data entries or usage scenarios.
func (k *TableKey) SetId(id string) *TableKey {
	k.id = id
	return k
}

// Prefix references internal constants to provide the domain prefix, typically derived
// from the notion of tables in the data store. This ensures the final key abides
// by established domain segmentation rules.
func (k *TableKey) Prefix() string {

	if len(k.name) == 0 {
		return PrefixTable
	}

	return PrefixTable + k.name + IdDelimiter
}

// Key merges the domain prefix with a particular identifier, reflecting a
// complete definition of a table-based key.
func (k *TableKey) Key() string {
	return k.Prefix() + k.id
}

// Base returns a concise representation of the table name and ID,
// useful for referencing combined pieces of TableKey logic in other contexts.
func (k *TableKey) Base() string {
	return k.name + IdDelimiter + k.id
}

// Equals determines whether two TableKeys share the same conceptual table name
// and identifier. This can facilitate equality checks within the domain logic.
func (k *TableKey) Equals(key *TableKey) bool {
	return k.name == key.name && k.id == key.id
}

//endregion

//region LinkKey

// LinkKey defines a conceptual link between two table domains (each represented by a TableKey).
// It is a convenient abstraction for referencing a relationship between entities.
type LinkKey struct {
	*baseKey
	currentTableKey *TableKey
	targetTableKey  *TableKey
}

// NewProtoLinkKey returns an initially empty LinkKey instance, allowing
// safe usage without nil references. This object can be customized later
// with specific TableKeys.
func NewProtoLinkKey() *LinkKey {
	k := &LinkKey{}
	k.baseKey = newBaseKey(k)
	return k
}

// NewLinkKeyFromString parses a raw string to discern two separate TableKey
// references, forming a LinkKey that holds a conceptual relationship between them.
// If parsing fails, a proto LinkKey is returned without set references.
func NewLinkKeyFromString(key string) *LinkKey {

	base, _ := strings.CutPrefix(key, PrefixLink)

	links := strings.Split(base, LinkDelimiter)
	if len(links) != 2 {
		return NewProtoLinkKey()
	}

	return NewLinkKey(NewTableKeyFromString(links[0]), NewTableKeyFromString(links[1]))
}

// NewLinkKey merges two TableKeys (current and target) to establish a single
// LinkKey entity. This abstracts a domain-specific relationship.
func NewLinkKey(current *TableKey, target *TableKey) *LinkKey {

	key := NewProtoLinkKey()
	key.currentTableKey = current
	key.targetTableKey = target

	return key
}

// CurrentTableKey returns the "current" side of the link relationship,
// usually signifying the source in a data relationship concept.
func (l *LinkKey) CurrentTableKey() *TableKey {
	return l.currentTableKey
}

// TargetTableKey returns the "target" side of the link relationship,
// indicating the destination or related entity.
func (l *LinkKey) TargetTableKey() *TableKey {
	return l.targetTableKey
}

// Prefix obtains the conceptual marker for links, leaving room for optional
// customization if different partitions are needed for relationship tracking.
func (l *LinkKey) Prefix() string {
	return PrefixLink
}

// Key constructs the complete representation of a link, merging
// the two TableKeys' base data with an internal delimiter to convey the relationship.
func (l *LinkKey) Key() string {
	return l.Prefix() +
		l.currentTableKey.Base() +
		LinkDelimiter +
		l.targetTableKey.Base()
}

//endregion
