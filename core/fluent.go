package core

import "github.com/Phosmachina/FluentKV/helper"

//region Base

// TableName returns the name of the table generated from the type parameter.
func TableName[T any]() string {
	var t T
	return helper.StructName(t)
}

// Insert stores a new value in the database and returns a wrapper around it.
// This is the primary way to insert a brand-new record of type T.
//
// Possible Error:
//   - ErrFailedToSet: If the underlying driver fails to store the data.
func Insert[T any](db *KVStoreManager, value *T) (KVWrapper[T], error) {

	valueAsAny := any(*value)
	tableKey, err := db.Insert(&valueAsAny)

	if err != nil {
		return KVWrapper[T]{}, err
	}

	return NewKVWrapper(db, tableKey, value), nil
}

// Set updates a value that must already exist, identified by a specific string ID.
// If the ID is valid, returns a new wrapper of the updated value.
//
// Possible Error:
//   - ErrInvalidId: If the specified ID is not recognized in the database.
//   - ErrFailedToSet: If the underlying driver fails to update the data.
func Set[T any](db *KVStoreManager, id string, value *T) (KVWrapper[T], error) {

	tableKey := NewTableKey[T]().SetId(id)
	valueAsAny := any(value)

	if err := db.Set(tableKey, &valueAsAny); err != nil {
		return KVWrapper[T]{}, err
	}

	return NewKVWrapper(db, tableKey, value), nil
}

// SetWrp is similar to Set, but takes a KVWrapper instead of separate parameters.
// It updates the wrapped object's value in the database.
func SetWrp[T any](objWrp KVWrapper[T]) (KVWrapper[T], error) {
	return Set(objWrp.db, objWrp.key.id, objWrp.value)
}

// Get retrieves a value from the database by its string ID. A successful call
// returns a wrapper containing the retrieved data.
//
// Possible Error:
//   - ErrInvalidId: If the specified ID is not found in the database.
func Get[T any](db *KVStoreManager, id string) (KVWrapper[T], error) {

	tableKey := NewTableKey[T]().SetId(id)
	value, err := db.Get(tableKey)

	if err != nil {
		return KVWrapper[T]{}, err
	}

	valueAsT := (*value).(T)

	return NewKVWrapper(db, tableKey, &valueAsT), nil
}

// Update fetches an existing record by ID, applies a user-defined function
// to modify the record, and writes the changes back to the database.
// Returns a wrapper around the updated value.
//
// Possible Error:
//   - ErrInvalidId: If the specified ID is not recognized in the database.
//   - ErrFailedToSet: If the underlying driver fails to update the modified data.
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

// Delete removes an object by its ID from the database.
// Once deleted, the ID is freed for future re-use and any related links are also removed.
//
// Possible Error:
//   - ErrInvalidId: If the specified ID is not found or cannot be removed.
func Delete[T any](db *KVStoreManager, id string) error {
	return db.Delete(NewTableKey[T]().SetId(id))
}

// DeleteWrp removes the object referenced by the wrapper from the database. Once deleted,
// its ID is freed and any related links are cleared.
func DeleteWrp[T any](objWrp KVWrapper[T]) error {
	return Delete[T](objWrp.db, objWrp.key.id)
}

// DeepDelete removes an object and all recursively linked objects in a single operation.
// Use this if you need to purge an object along with all its connections.
//
// Possible Error:
//   - ErrInvalidId: If the specified ID is not recognized.
func DeepDelete[T any](db *KVStoreManager, id string) error {
	return db.DeepDelete(NewTableKey[T]().SetId(id))
}

// DeepDeleteWrp removes the object in the wrapper and all recursively linked objects.
func DeepDeleteWrp[T any](objWrp KVWrapper[T]) error {
	return DeepDelete[T](objWrp.db, objWrp.key.id)
}

// Exist checks if an object with the given ID is present in the database.
// Returns true if found, false otherwise.
func Exist[T any](db *KVStoreManager, id string) bool {
	return db.Exist(NewTableKey[T]().SetId(id))
}

// ExistWrp is the wrapper-based version of Exist, checking if the wrapped object
// exists in the database.
func ExistWrp[T any](objWrp KVWrapper[T]) bool {
	return Exist[T](objWrp.db, objWrp.key.id)
}

// Count returns the total number of objects for the type T stored in the database.
func Count[T any](db *KVStoreManager) int {
	return db.Count(NewTableKey[T]())
}

// Foreach iterates over all objects of type T in the database, calling the provided
// function for each item. The do callback supplies both the key and the typed value.
func Foreach[T any](db *KVStoreManager, do func(key IKey, value *T)) {
	db.Foreach(NewTableKey[T](), func(key *TableKey, value *any) {
		t := (*value).(T)
		do(key, &t)
	})
}

// FindFirst iterates through all objects of type T, invoking the predicate function
// until it matches (returns true). The matching item is returned as a KVWrapper.
// If no match is found, returns an empty wrapper.
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

// FindAll collects all objects of type T matching the predicate. Each result
// is returned as a KVWrapper for further manipulation or inspection.
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

// Link creates one or more links from the Current object to one or more Target objects.
// If biDirectional is true, the link is also established in reverse.
//
// Possible Errors:
//   - ErrInvalidId: If the current or any target ID is not recognized in the database.
//   - ErrSelfBind: If Current and Target refer to the same object.
//   - ErrFailedToSet: If the underlying driver fails to record the link.
func Link[Current any, Target any](
	current KVWrapper[Current],
	biDirectional bool,
	targets ...KVWrapper[Target],
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

// LinkNew works like Link, but first inserts each provided target object into the database,
// then creates the links.
// Returns a collection of wrappers for the newly inserted and linked target objects.
//
// Errors encountered during Insert or linking are silently skipped for individual targets.
// For instance, if inserting a target fails, that target is not linked.
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
			// Skip if unable to insert
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

// CollectLinked retrieves all objects of type Target
// that are linked from a Current object with the provided ID.
// This is helpful for exploring graph or relational data.
func CollectLinked[Current any, Target any](
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

// CollectLinkedWrp is a wrapper-based variant of CollectLinked, fetching all linked Target objects
// from the Current object’s wrapper.
func CollectLinkedWrp[Current any, Target any](
	current KVWrapper[Current],
) []KVWrapper[Target] {
	return CollectLinked[Current, Target](current.db, current.key.id)
}

// Unlink removes any links between two objects, both the forward link (Current -> Target)
// and the backward link (Target -> Current), if it exists. Returns true if at least one link
// was successfully removed.
func Unlink[Current any, Target any](db KVDriver, idOfC string, idOfT string) bool {
	currentTableKey := NewTableKey[Current]().SetId(idOfC)
	targetCurrentKey := NewTableKey[Target]().SetId(idOfT)

	return db.RawDelete(NewLinkKey(targetCurrentKey, currentTableKey)) ||
		db.RawDelete(NewLinkKey(currentTableKey, targetCurrentKey))
}

// UnlinkWrp does the same as Unlink, but with wrapper types for convenience.
func UnlinkWrp[Current any, Target any](
	current KVWrapper[Current],
	target KVWrapper[Target],
) bool {
	return Unlink[Current, Target](current.db, current.key.id, target.key.id)
}

// UnlinkAllTarget removes all links between an object identified by id
// and objects in the specified Target table.
func UnlinkAllTarget[Current any, Target any](db KVDriver, id string) {

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

// UnlinkAllTargetWrp is the wrapper-based counterpart of UnlinkAllTarget.
func UnlinkAllTargetWrp[Current any, Target any](current KVWrapper[Current]) {
	UnlinkAllTarget[Current, Target](current.db, current.key.id)
}

// UnlinkAll removes every link connected to the specified object, effectively
// disconnecting it from all related records.
func UnlinkAll[Current any](db KVDriver, id string) {

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

// UnlinkAllWrp is a wrapper-based version of UnlinkAll, removing all links
// from the object in the provided wrapper.
func UnlinkAllWrp[Current any](current KVWrapper[Current]) {
	UnlinkAll[Current](current.db, current.key.id)
}

// CollectAllLinkedKey scans all linked objects
// (both inbound and outbound links)
// of a given object and returns their IDs without retrieving the actual values.
func CollectAllLinkedKey[Current any](db KVDriver, currentId string) []*TableKey {

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

// CollectAllLinkedKeyWrp is the wrapper-based variant of CollectAllLinkedKey,
// returning the IDs of all linked objects.
func CollectAllLinkedKeyWrp[Current any](s *KVWrapper[Current]) []*TableKey {
	return CollectAllLinkedKey[Current](s.db, s.key.id)
}

//endregion

//region Collections

// Where ; this function considers a collection and the connected collection induced by
// Link (probably used in the same way for each element of the collection).
//
// For each object in the collection the predicate is applied with the current object and all
// connected objects given by a CollectLinked operation.
// If the predicate returns true, the current object is retained.
//
// If the option firstOnly is set to true, only the first result of CollectLinked is retained.
//
// The result of unlinking can be nil, and the predicate function must manage this case.
func Where[T any, K any](
	firstOnly bool,
	collection *Collection[T],
	predicate func(objWrp1 KVWrapper[T], objWrp2 KVWrapper[K]) bool,
) *Collection[T] {

	var list []KVWrapper[T]
	for _, objCol1 := range collection.objects {
		objectsWrp := CollectLinked[T, K](objCol1.db, objCol1.key.id)
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
