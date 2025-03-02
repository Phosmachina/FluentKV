package core

type KVWrapper[T any] struct {
	db    *KVStoreManager
	key   *TableKey
	value *T
}

func (w *KVWrapper[T]) IsEmpty() bool {
	return w.key == nil && w.value == nil
}

func (w *KVWrapper[T]) Key() *TableKey {
	return w.key
}

func (w *KVWrapper[T]) Value() *T {
	return w.value
}

func NewKVWrapper[T any](
	db *KVStoreManager,
	key *TableKey,
	value *T,
) KVWrapper[T] {
	return KVWrapper[T]{db: db, key: key, value: value}
}

// TODO rename all S and T by something more intuitive like 'current' and 'target'.
