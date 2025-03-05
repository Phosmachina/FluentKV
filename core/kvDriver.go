package core

// KVDriver defines the core low-level database operations necessary for a key-value store.
//
// Each stored entry is identified by an IKey, which comprises a prefix (e.g., an internal
// data-type indicator) and other identifying parts (e.g., a table name and/or unique ID).
// This interface focuses on direct read, write, and iteration operations, leaving more complex
// logic (like marshalling or triggers) to higher-level abstractions.
type KVDriver interface {

	// RawSet stores the given byte slice (value) under the specified key.
	// This method does not verify if the key is currently in use. If the call fails,
	// it returns false.
	RawSet(key IKey, value []byte) bool

	// RawGet retrieves the byte slice (value) associated with the given key.
	// If the key does not exist, it returns an empty slice and false.
	// If found, it returns the stored byte slice and true.
	RawGet(key IKey) ([]byte, bool)

	// RawDelete removes the entry identified by the given key from the storage.
	// Returns true if the deletion is successful, otherwise false.
	RawDelete(key IKey) bool

	// RawIterKey scans the storage for items whose key begins with the prefix defined
	// by the given key, then invokes the provided action function on each matching key.
	// If action returns true, iteration stops immediately. This method does not fetch values,
	// only keys.
	RawIterKey(key IKey, action func(key IKey) (stop bool))

	// RawIterKV behaves like RawIterKey but also retrieves and passes the associated values
	// to the action function. If action returns true, iteration stops prematurely.
	RawIterKV(key IKey, action func(key IKey, value []byte) (stop bool))

	// Exist checks if an entry exists for the specified key. It returns true
	// if the key is found, false otherwise.
	Exist(key IKey) bool

	// Close signals the driver to release any held resources and prevents further use.
	// Once Close is called, subsequent method calls are not guaranteed to succeed.
	Close()
}
