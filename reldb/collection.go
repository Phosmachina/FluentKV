package fluentkv

import (
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

type Collection[T IObject] struct {
	objects    []*ObjWrapper[T]
	comparator func(x, y *ObjWrapper[T]) bool
}

func NewCollection[T IObject](db IRelationalDB) *Collection[T] {
	var list []*ObjWrapper[T]
	Foreach[T](db, func(id string, value *T) {
		list = append(list, NewObjWrapper(db, id, value))
	})
	return &Collection[T]{objects: list}
}

// GetArray give the underlying ObjWrapper array of the current collection.
func (c *Collection[T]) GetArray() []*ObjWrapper[T] {
	return c.objects
}

func (c *Collection[T]) Sort(
	comparator func(x, y *ObjWrapper[T]) bool,
) *Collection[T] {

	c.comparator = comparator
	sort.Sort(c)

	return c
}

// Distinct ; simply eliminate all duplicate from the collection.
//
// The underlying ObjWrapper array is modified with this operation.
func (c *Collection[T]) Distinct() *Collection[T] {
	allKeys := make(map[string]bool)
	var list []*ObjWrapper[T]
	for _, object := range c.objects {
		if _, value := allKeys[object.Value.Hash()]; !value {
			allKeys[object.Value.Hash()] = true
			list = append(list, object)
		}
	}
	c.objects = list
	return c
}

// Where ; this function consider a collection and the connected collection induced by Link (
// probably used in a same way for each element of the collection).
//
// For each object in collection the predicate is applied with the current object and all
// connected object given by an AllFromLink operation. If the predicate returns true,
// the current object is retained.
//
// If the option firstOnly is set to true, only the first result of AllFromLink is retained.
//
// Obviously, the result of unlink can be nil and predicate function must manage this case.
func Where[T IObject, K IObject](
	firstOnly bool,
	collection *Collection[T],
	predicate func(objWrp1 *ObjWrapper[T], objWrp2 *ObjWrapper[K]) bool,
) *Collection[T] {

	var list []*ObjWrapper[T]
	for _, objCol1 := range collection.objects {
		objectsWrp := AllFromLink[T, K](objCol1.db, objCol1.ID)
		if len(objectsWrp) > 1 && firstOnly {
			objectsWrp = objectsWrp[1:]
		}
		for _, objCol2 := range objectsWrp {
			if predicate(objCol1, objCol2) {
				list = append(list, objCol1)
				break
			}
		}
	}
	collection.objects = list

	return collection
}

// Filter iterate on the collection and for each object apply the predicate.
// If the result is true, the object is retained.
//
// Note: technically you can use this function to get the same result as Where operation.
//
// The underlying ObjWrapper array is modified with this operation.
func (c *Collection[T]) Filter(
	predicate func(objWrp *ObjWrapper[T]) bool,
) *Collection[T] {

	var list []*ObjWrapper[T]
	for _, objWrp := range c.objects {
		if predicate(objWrp) {
			list = append(list, objWrp)
		}
	}

	c.objects = list
	return c
}
