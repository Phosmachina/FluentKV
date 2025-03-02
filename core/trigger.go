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
	Id() string
	TableName() string
	Operation() Operation
	IsBefore() bool
	StartBefore(Operation, IKey, *any) bool
	StartAfter(Operation, IKey, *any)
	Equals(ITrigger) bool
}

type trigger struct {
	id         string
	tableName  string
	operations Operation
	isBefore   bool
	beforeTask func(operation Operation, key IKey, value *any) bool
	afterTask  func(operation Operation, key IKey, value *any)
}

func (t trigger) Id() string {
	return t.id
}

func (t trigger) TableName() string {
	return t.tableName
}

func (t trigger) Operation() Operation {
	return t.operations
}

func (t trigger) IsBefore() bool {
	return t.isBefore
}

func (t trigger) StartBefore(operation Operation, key IKey, value *any) bool {
	return t.beforeTask(operation, key, value)
}

func (t trigger) StartAfter(operation Operation, key IKey, value *any) {
	t.afterTask(operation, key, value)
}

func (t trigger) Equals(other ITrigger) bool {
	return t.TableName() == other.TableName() && t.Id() == other.Id()
}

func indexOf(t ITrigger, triggers []ITrigger) int {
	for k, v := range triggers {
		if t.Equals(v) {
			return k
		}
	}
	return -1 // not found.
}
