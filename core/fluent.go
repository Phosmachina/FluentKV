package core

import "github.com/Phosmachina/FluentKV/helper"

//region Base

// TableName is a helper to call the TableName from T any implementation.
func TableName[T any]() string {
	var t T
	return helper.StructName(t)
}

// Insert in the db the value and return the resulting wrapper.
func Insert[T any](db *KVStoreManager, value *T) (KVWrapper[T], error) {

	valueAsAny := any(*value)
	tableKey, err := db.Insert(&valueAsAny)
	if err != nil {
		return KVWrapper[T]{}, err
	}

	return NewKVWrapper(db, tableKey, value), nil
}

// Set override the value at Id specified with the passed value. The Id shall exist.
func Set[T any](db *KVStoreManager, id string, value *T) (KVWrapper[T], error) {

	tableKey := NewTableKey[T]().SetId(id)

	valueAsAny := any(value)
	if err := db.Set(tableKey, &valueAsAny); err != nil {
		return KVWrapper[T]{}, err
	}

	return NewKVWrapper(db, tableKey, value), nil
}

// SetWrp same as Set but take a wrapped object in argument.
func SetWrp[T any](objWrp KVWrapper[T]) (KVWrapper[T], error) {
	return Set(objWrp.db, objWrp.key.id, objWrp.value)
}

// Get the value in db based on Id and the tableName induced by the T parameter.
func Get[T any](db *KVStoreManager, id string) (KVWrapper[T], error) {

	tableKey := NewTableKey[T]().SetId(id)
	value, err := db.Get(tableKey)

	if err != nil {
		return KVWrapper[T]{}, err
	}

	valueAsT := (*value).(T)

	return NewKVWrapper(db, tableKey, &valueAsT), nil
}

// Update the value determine with the Id and the tableName induced by the T parameter.
// The result of the editor function is set in the db.
func Update[T any](
	db *KVStoreManager,
	id string,
	editor func(value *T),
) (KVWrapper[T], error) {

	var valueAsT T
	tableKey := NewTableKey[T]().SetId(id)

	_, err := db.Update(tableKey, func(value *any) *any {
		valueAsT = (*value).(T)
		editor(&valueAsT)
		valueAsAny := any(valueAsT)
		return &valueAsAny
	})
	if err != nil {
		return KVWrapper[T]{}, err
	}

	return NewKVWrapper(db, tableKey, &valueAsT), nil
}

// Delete the object determined with the Id and the tableName induced by the T parameter.
// Id is released and related links are deleted.
func Delete[T any](db *KVStoreManager, id string) error {
	return db.Delete(NewTableKey[T]().SetId(id))
}

// DeleteWrp the object determined with the Id and the tableName induced by the T
// parameter.
// Id is released and related links are deleted.
func DeleteWrp[T any](objWrp KVWrapper[T]) error {
	return Delete[T](objWrp.db, objWrp.key.id)
}

// DeepDelete the object determined with the Id and the tableName induced by the T
// parameter and all object directly connected.
func DeepDelete[T any](db *KVStoreManager, id string) error {
	return db.DeepDelete(NewTableKey[T]().SetId(id))
}

// DeepDeleteWrp the object determined with the Id and the tableName induced by the T
// parameter and all object directly connected.
func DeepDeleteWrp[T any](objWrp KVWrapper[T]) error {
	return DeepDelete[T](objWrp.db, objWrp.key.id)
}

// Exist return true if the object determined with the Id and the tableName induced by the
// T parameter exist in db.
func Exist[T any](db *KVStoreManager, id string) bool {
	return db.Exist(NewTableKey[T]().SetId(id))
}

// ExistWrp return true if the object determines with the Id and the tableName induced by
// the T parameter exist in db.
func ExistWrp[T any](objWrp KVWrapper[T]) bool {
	return Exist[T](objWrp.db, objWrp.key.id)
}

// Count return the count of objects in the table based on the tableName induced by the T
// parameter.
func Count[T any](db *KVStoreManager) int {
	return db.Count(NewTableKey[T]())
}

// Foreach iterating on the table based on the tableName induced by the T parameter
// and execute the do function on each value.
func Foreach[T any](db *KVStoreManager, do func(key IKey, value *T)) {
	db.Foreach(NewTableKey[T](), func(key *TableKey, value *any) {
		t := (*value).(T)
		do(key, &t)
	})
}

// FindFirst iterate on the table based on the tableName induced by the T parameter and execute the
// predicate function on each value until it return true value: the current value is returned.
func FindFirst[T any](
	db *KVStoreManager,
	predicate func(key *TableKey, value *T) bool,
) KVWrapper[T] {

	tableKey, resultValue := db.FindFirst(
		NewTableKey[T](),
		func(key *TableKey, value *any) bool {
			t := (*value).(T)
			return predicate(key, &t)
		},
	)

	if resultValue == nil {
		return KVWrapper[T]{}
	}

	resultValueAsT := (*resultValue).(T)

	return NewKVWrapper(db, tableKey, &resultValueAsT)
}

// FindAll iterate on the table based on the tableName induced by the T parameter and
// execute the predicate function on each value. All values matching the predicate
// are returned.
func FindAll[T any](
	db *KVStoreManager,
	predicate func(key *TableKey, value *T) bool,
) []KVWrapper[T] {

	var objs []KVWrapper[T]

	tableKeys, results := db.FindAll(
		NewTableKey[T](),
		func(key *TableKey, value *any) bool {
			t := (*value).(T)
			return predicate(key, &t)
		},
	)

	for i, tableKey := range tableKeys {
		t := (*results[i]).(T)
		objs = append(objs, NewKVWrapper(db, tableKey, &t))
	}

	return objs
}

//endregion

//region Links

// Link add a link between s object and all targets objects.
// The biDirectional attribute determines if for target is also connected to s:
//
//		 biDirectional == false: s -> t
//
//		 biDirectional == true:  s -> t
//	                          s <- t
func Link[C any, T any](
	current KVWrapper[C],
	biDirectional bool,
	targets ...KVWrapper[T],
) error {

	if !ExistWrp(current) {
		return ErrInvalidId
	}

	for _, target := range targets {
		exist := current.db.Exist(target.key)

		if !exist {
			return ErrInvalidId
		}

		if target.key.Id() == current.key.Id() {
			return ErrSelfBind
		}

		if biDirectional {
			if !current.db.RawSet(NewLinkKey(target.key, current.key), nil) {
				return ErrFailedToSet
			}
		}

		if !current.db.RawSet(NewLinkKey(current.key, target.key), nil) {
			return ErrFailedToSet
		}
	}

	return nil
}

// LinkNew same as Link but take any array and insert them in the db,
// then return the resulting wrapping.
func LinkNew[Current any, Target any](
	current KVWrapper[Current],
	biDirectional bool,
	targets ...*Target,
) []KVWrapper[Target] {

	var targetsWrp []KVWrapper[Target]

	if !ExistWrp(current) {
		return targetsWrp
	}

	for _, target := range targets {
		object := any(*target)
		tableKey, err := current.db.Insert(&object)
		if err != nil {
			continue
		}

		targetWrp := NewKVWrapper(current.db, tableKey, target)
		_ = Link[Current, Target](current, biDirectional, targetWrp)
		targetsWrp = append(targetsWrp, targetWrp)
	}

	return targetsWrp
}

// TODO potentially make FromLink method
// TODO: Consider implementing a cardinality feature to
//  manage the number of links or constraints on relationships.

// AllFromLink returned all objects, with the tableName induced by T, connected to the S object.
func AllFromLink[Current any, Target any](
	db *KVStoreManager,
	currentId string,
) []KVWrapper[Target] {

	var targetsWrp []KVWrapper[Target]
	tableKey := NewTableKey[Current]().SetId(currentId)
	targetTableName := NewTableKey[Target]().name

	db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {
		linkKey := key.(*LinkKey)

		if linkKey.currentTableKey.Equals(tableKey) &&
			linkKey.targetTableKey.name == targetTableName {

			object, _ := db.Get(linkKey.targetTableKey)
			value := (*object).(Target)

			targetsWrp = append(
				targetsWrp,
				NewKVWrapper[Target](db, linkKey.targetTableKey, &value),
			)
		}

		return false
	})

	return targetsWrp
}

// AllFromLinkWrp returned all objects, with the tableName induced by T, connected to the S object.
func AllFromLinkWrp[Current any, Target any](
	current KVWrapper[Current],
) []KVWrapper[Target] {
	return AllFromLink[Current, Target](current.db, current.key.id)
}

// RemoveLink remove all link between s and t object. Return true if links s->t are deleted (is
// at least the link created when isBidirectional == false).
func RemoveLink[S any, T any](db KVDriver, idOfS string, idOfT string) bool {

	currentTableKey := NewTableKey[S]().SetId(idOfS)
	targetCurrentKey := NewTableKey[T]().SetId(idOfT)

	db.RawDelete(NewLinkKey(targetCurrentKey, currentTableKey))

	return db.RawDelete(NewLinkKey(currentTableKey, targetCurrentKey))
}

// RemoveLinkWrp remove all link between s and t object. Return true if links s->t are deleted (is
// at least the link created when isBidirectional == false).
func RemoveLinkWrp[Current any, Target any](
	current KVWrapper[Current],
	target KVWrapper[Target],
) bool {
	return RemoveLink[Current, Target](current.db, current.key.id, target.key.id)
}

// RemoveAllTableLink remove all link between t object and object having the S tableName.
func RemoveAllTableLink[Current any, Target any](db KVDriver, id string) {

	db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {

		linkKey := key.(*LinkKey)
		currentTableKey := NewTableKey[Current]().SetId(id)
		targetTableName := NewTableKey[Target]().name

		if linkKey.currentTableKey.Equals(currentTableKey) &&
			linkKey.targetTableKey.name == targetTableName ||
			linkKey.targetTableKey.Equals(currentTableKey) &&
				linkKey.currentTableKey.name == targetTableName {

			db.RawDelete(linkKey)
		}

		return false
	})
}

// RemoveAllTableLinkWrp remove all link between t object and object having the S tableName.
func RemoveAllTableLinkWrp[Current any, Target any](current KVWrapper[Current]) {
	RemoveAllTableLink[Current, Target](current.db, current.key.id)
}

// RemoveAllLink remove all link connected to this object.
func RemoveAllLink[Current any](db KVDriver, id string) {

	db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {

		linkKey := key.(*LinkKey)
		currentTableKey := NewTableKey[Current]().SetId(id)

		if linkKey.currentTableKey.Equals(currentTableKey) ||
			linkKey.targetTableKey.Equals(currentTableKey) {

			db.RawDelete(linkKey)
		}

		return false
	})
}

// RemoveAllLinkWrp remove all link connected to this object.
func RemoveAllLinkWrp[Current any](current KVWrapper[Current]) {
	RemoveAllLink[Current](current.db, current.key.id)
}

// TODO re-organize all public methods. Why these specific methods are in object.go file?
// TODO there is a function to iterate through a table without value retrieving?
// TODO make the Visit method test. Maybe rename it with "iter" like other one

// Visit iterate on all connected objects and returns all ids. Prevent the value recovering.
func Visit[Current any](db KVDriver, currentId string) []*TableKey {

	var tableKeys []*TableKey

	db.RawIterKey(NewProtoLinkKey(), func(key IKey) (stop bool) {

		linkKey := key.(*LinkKey)
		currentTableKey := NewTableKey[Current]().SetId(currentId)

		if linkKey.targetTableKey.Equals(currentTableKey) {
			tableKeys = append(tableKeys, linkKey.targetTableKey)
		} else if linkKey.currentTableKey.Equals(currentTableKey) {
			tableKeys = append(tableKeys, linkKey.currentTableKey)
		}

		return false
	})

	return tableKeys
}

// VisitWrp iterate on all connected objects and return all ids.
// Prevent the value from being retrieved.
func VisitWrp[Current any](s *KVWrapper[Current]) []*TableKey {
	return Visit[Current](s.db, s.key.id)
}

//endregion

//region Collections

// Where ; this function considers a collection and the connected collection induced by
// Link (probably used in a same way for each element of the collection).
//
// For each object in the collection the predicate is applied with the current object and all
// connected object given by an AllFromLink operation. If the predicate returns true,
// the current object is retained.
//
// If the option firstOnly is set to true, only the first result of AllFromLink is retained.
//
// The result of unlink can be nil, and the predicate function must manage this case.
func Where[T any, K any](
	firstOnly bool,
	collection *Collection[T],
	predicate func(objWrp1 KVWrapper[T], objWrp2 KVWrapper[K]) bool,
) *Collection[T] {

	var list []KVWrapper[T]
	for _, objCol1 := range collection.objects {
		objectsWrp := AllFromLink[T, K](objCol1.db, objCol1.key.id)
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
// The underlying KVWrapper array is modified with this operation.
func (c *Collection[T]) Filter(
	predicate func(objWrp KVWrapper[T]) bool,
) *Collection[T] {

	var list []KVWrapper[T]
	for _, objWrp := range c.objects {
		if predicate(objWrp) {
			list = append(list, objWrp)
		}
	}
	c.objects = list

	return c
}

//endregion

//region Triggers

// AddBeforeTrigger register a new trigger with the given parameter and table name
// inferred from the T parameter.
// The action ran before the targeted operation.
//
// Parameters:
//
// Id: a string that will be used as the identifier of the new trigger: it could be a
// description but should be unique relatively to the table name.
//
// operations: a value of type Operation defining the operations that will trigger the
// action.
//
// action: a function that will be executed when the trigger fires with the Id and
// value of the current operation.
// The return value, an error,
// is used as a condition for performing the targeted operation.
//
// Returns ErrDuplicateTrigger if a trigger with the same Id already exists in the
// provided database.
func AddBeforeTrigger[T any](
	db *KVStoreManager,
	id string,
	operations Operation,
	action func(operation Operation, key IKey, value *T) bool,
) error {

	triggerToBeAdded := trigger{
		id:         id,
		tableName:  TableName[T](),
		operations: operations,
		isBefore:   true,
		beforeTask: func(operation Operation, key IKey, value *any) bool {
			valueAsT := (*value).(T)
			return action(operation, key, &valueAsT)
		},
	}

	db.m.Lock()

	index := indexOf(triggerToBeAdded, db.triggers)
	if index != -1 {
		return ErrDuplicateTrigger
	}

	db.triggers = append(db.triggers, triggerToBeAdded)

	db.m.Unlock()

	return nil
}

// AddAfterTrigger register a new trigger with the given parameter and table name
// inferred from the T parameter.
// The action ran after the targeted operation.
//
// Parameters:
//
// Id: a string that will be used as the identifier of the new trigger: it could be a
// description but should be unique relatively to the table name.
//
// operations: a value of type Operation defining the operations that will trigger the
// action.
//
// action: a function that will be executed when the trigger fires with the Id and
// value of the current operation.
//
// Returns ErrDuplicateTrigger if a trigger with the same Id already exists in the
// provided database.
func AddAfterTrigger[T any](
	db *KVStoreManager,
	id string,
	operations Operation,
	action func(operation Operation, key IKey, value *T),
) error {

	triggerToBeAdded := trigger{
		id:         id,
		tableName:  TableName[T](),
		operations: operations,
		isBefore:   false,
		afterTask: func(operation Operation, key IKey, value *any) {
			valueAsAny := (*value).(T)
			action(operation, key, &valueAsAny)
		},
	}

	db.m.Lock()

	if indexOf(triggerToBeAdded, db.triggers) != -1 {
		return ErrDuplicateTrigger
	}
	db.triggers = append(db.triggers, triggerToBeAdded)

	db.m.Unlock()

	return nil
}

// DeleteTrigger deletes a trigger from registered triggers based on the table name
// inferred by the T parameter and the Id.
//
// If the trigger is not present, it returns ErrInexistantTrigger.
func DeleteTrigger[T any](db *KVStoreManager, id string) error {

	triggerToBeRemoved := trigger{
		id:        id,
		tableName: TableName[T](),
	}

	db.m.Lock()

	index := indexOf(triggerToBeRemoved, db.triggers)
	if index == -1 {
		return ErrInexistantTrigger
	}

	db.triggers = append(db.triggers[:index], db.triggers[index+1:]...)

	db.m.Unlock()

	return nil
}

//endregion
