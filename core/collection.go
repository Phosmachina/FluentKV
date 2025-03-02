package core

import (
	. "github.com/Phosmachina/FluentKV/helper"
	"sort"
)

func (c *Collection[T]) Len() int {
	return len(c.objects)
}

func (c *Collection[T]) Less(i, j int) bool {
	return c.comparator(c.objects[i], c.objects[j])
}

func (c *Collection[T]) Swap(i, j int) {
	c.objects[i], c.objects[j] = c.objects[j], c.objects[i]
}

type Collection[T any] struct {
	objects    []KVWrapper[T]
	comparator func(x, y KVWrapper[T]) bool
}

func NewCollection[T any](db *KVStoreManager) *Collection[T] {

	var list []KVWrapper[T]
	Foreach[T](db, func(key IKey, value *T) {
		list = append(list, NewKVWrapper(db, key.(*TableKey), value))
	})

	return &Collection[T]{objects: list}
}

// GetArray give the underlying KVWrapper array of the current collection.
func (c *Collection[T]) GetArray() []KVWrapper[T] {
	return c.objects
}

// Sort the collection with the given function used as comparator for each element.
func (c *Collection[T]) Sort(
	comparator func(x, y KVWrapper[T]) bool,
) *Collection[T] {

	c.comparator = comparator
	sort.Sort(c)

	return c
}

// Distinct eliminate all duplicates from the collection.
//
// The underlying KVWrapper array is modified with this operation.
func (c *Collection[T]) Distinct() *Collection[T] {

	allKeys := make(map[string]bool)

	var list []KVWrapper[T]
	for _, object := range c.objects {

		hash := Hash(object.Value())
		if _, value := allKeys[hash]; !value {
			allKeys[hash] = true
			list = append(list, object)
		}
	}
	c.objects = list

	return c
}
