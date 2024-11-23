package implementation

import (
	. "github.com/Phosmachina/FluentKV/core"
	"strings"
)

type Generic struct {
	AbstractRelDB
	store map[string][]byte
}

func NewGeneric() *Generic {

	db := &Generic{store: make(map[string][]byte)}
	db.AbstractRelDB = *NewAbstractRelDB(db)

	return db
}

// region IRelationalDB implementation

func (db *Generic) RawSet(key IKey, value []byte) bool {
	db.store[key.Key()] = value
	return true
}

func (db *Generic) RawGet(key IKey) ([]byte, bool) {
	value, ok := db.store[key.Key()]
	return value, ok
}

func (db *Generic) RawDelete(key IKey) bool {
	delete(db.store, key.Key())
	return true
}

func (db *Generic) RawIterKey(
	currentKey IKey,
	action func(key IKey) (stop bool),
) {

	for k := range db.store {
		if !strings.HasPrefix(k, currentKey.Prefix()) {
			continue
		}

		if action(NewKeyFromString(k)) {
			break
		}
	}
}

func (db *Generic) RawIterKV(
	key IKey,
	action func(key IKey, value []byte) (stop bool),
) {

	for currentKey, value := range db.store {
		if !strings.HasPrefix(currentKey, key.Prefix()) {
			continue
		}

		if action(NewKeyFromString(currentKey), value) {
			break
		}
	}
}

func (db *Generic) Exist(key IKey) bool {
	_, ok := db.store[key.Key()]
	return ok
}

// endregion
