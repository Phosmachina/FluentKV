package core

import "errors"

var (
	ErrCancelledByTrigger = errors.New("a trigger executed before cancelled the operation")
	ErrDuplicateTrigger   = errors.New("the trigger with same Id for this table name is already added")
	ErrInexistantTrigger  = errors.New("the trigger does not exist")
)

// Operation represents CRUD operations in bitmask format.
type Operation int

// Contain checks if the Operation in parameter is present in this Operation.
func (o Operation) Contain(s Operation) bool {
	return o&s == s
}

// CRUD Operation used for trigger as a filter.
const (
	GetOperation Operation = 1 << iota
	InsertOperation
	DeleteOperation
	UpdateOperation
)

type ITrigger interface {
	GetId() string
	GetTableName() string
	GetOperation() Operation
	IsBefore() bool
	StartBefore(Operation, IKey, *IObject) bool
	StartAfter(Operation, IKey, *IObject)
	Equals(other ITrigger) bool
}

type trigger[T IObject] struct {
	id         string
	tableName  string
	operations Operation
	isBefore   bool
	beforeTask func(operation Operation, key IKey, value *T) bool
	afterTask  func(operation Operation, key IKey, value *T)
}

func (t trigger[T]) GetId() string {
	return t.id
}

func (t trigger[T]) GetTableName() string {
	return t.tableName
}
func (t trigger[T]) GetOperation() Operation {
	return t.operations
}

func (t trigger[T]) IsBefore() bool {
	return t.isBefore
}

func (t trigger[T]) StartBefore(operation Operation, key IKey, value *IObject) bool {
	v := (*value).(T)
	return t.beforeTask(operation, key, &v)
}

func (t trigger[T]) StartAfter(operation Operation, key IKey, value *IObject) {
	v := (*value).(T)
	t.afterTask(operation, key, &v)
}

func (t trigger[T]) Equals(other ITrigger) bool {
	return t.GetTableName() == other.GetTableName() && t.GetId() == other.GetId()
}

func indexOf(t ITrigger, triggers []ITrigger) int {
	for k, v := range triggers {
		if t.Equals(v) {
			return k
		}
	}
	return -1 // not found.
}
