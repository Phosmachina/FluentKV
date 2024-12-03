package core

/*
KVDriver is a small interface to define some operation with a storage used like a relational DB.

# Key

At first, the underlying storage should work like a KV DB. In consequence,
a key is structured to store a flattened hierarchy.
  - Key: internalTypePrefix, suffix
  - Key for concrete types: internalTypePrefix, tableName, Id

To make uniq key, a tank key system is implemented and can be used with GetKey,
FreeKey. The global AutoIdBuffer defined the size of this tank. When the value is inserted,
a key is picked in the tank. After deleting an entry, the key becomes available again via GetKey.

# Operators

Interface is designed so that all raw operators must be implemented; other can be but are already
implemented in the abstraction KVStoreManager. Raw Operators probably work directly with the db driver and are used by all other operators.
*/
type KVDriver interface {

	// RawSet set a value in DB using the specified key.
	// Don't care about Key is already in use or not. Return false when the operation failed.
	RawSet(key IKey, value []byte) bool
	// RawGet get a value in DB using the specified key.
	// If no value corresponding to this Key, empty slice and false should be returned.
	RawGet(key IKey) ([]byte, bool)
	// RawDelete Deletes a value in the DB using the specified key.
	// Return true if the value is correctly deleted.
	RawDelete(key IKey) bool
	// RawIterKey iterate in DB when prefix match with Key.
	// The action is called for each key, and the key is truncated with the prefix given.
	// The stop boolean defined if iteration should be stopped.
	// No values are prefetched with this iterator.
	RawIterKey(key IKey, action func(key IKey) (stop bool))
	// RawIterKV iterate in DB when the prefix matches with the key.
	// The action is called for each key, and the key is truncated with the prefix given.
	// The stop boolean defined if iteration should be stopped.
	// Value is the corresponding value of the key.
	// TODO maybe rename to remove "Key/KV" ; use "Iter[WithValue]Over"
	RawIterKV(key IKey, action func(key IKey, value []byte) (stop bool))
	// Exist return true if the for corresponding TableName and ID exist in DB.
	Exist(key IKey) bool
	// Close, set the db to the closed state and finally close the db.
	Close()
}
