
<h1 align="center">
    <img src="docs/FluentKV_banner.svg" alt="Banner" style="text-align: center; width: 70%">
</h1>

<div align="center">

[![GoDoc](https://godoc.org/github.com/Phosmachina/FluentKV?status.svg)](https://pkg.go.dev/github.com/Phosmachina/FluentKV#section-documentation)
[![Go Report Card](https://goreportcard.com/badge/github.com/Phosmachina/FluentKV)](https://goreportcard.com/badge/github.com/Phosmachina/FluentKV)

</div>

> Purpose a fluent toolkit for using a KV database.

[//]: # (## 🎯 Overview)

[//]: # (## ⚡️ Features)

[//]: # (## 🚀 Getting started)

[//]: # (## 🤝 Contributing)

[//]: # (## 🕘 What's next)

[//]: # (---)

## System Architecture

### Interface and Abstract Struct Implementation

The interface **IRelationalDB** defines a set of raw operations that must be implemented.
It also holds some additional simpler operations already realized, but they can be
overridden if required. For further details, refer to the file `core/relational.go`.

The abstract struct **AbstractRelDB** extends **IRelationalDB**. It pre-implements the key
filler system and all other operations using raw operations provided by the
implementation. Check the file `core/abstract.go` for a comprehensive understanding.

### IObject Interface and ObjWrapper Struct

The `IObject` interface contains operations essential for this architecture. These
operations, such as `Equals`, `Hash`, are primarily implemented with the `DBObject`
abstraction but the `TableName` function needs explicit implementation. This function
generates a key corresponding to data.

The `ObjWrapper` struct is a helper to store a value along with its unique id. It is used
extensively across this toolkit to bind the IObject with the database or to return the
value with its corresponding id.

### Implementation

At present, there is one main implementation for a KV Database:

- [BadgerDB](https://github.com/dgraph-io/badger)
- [Redis](https://redis.io) (This is currently being considered)

### Collection

> A straightforward way of performing operations on a list of items provided by a 
> table name.

In SQL, certain operations are performed on a list of rows, usually fetched using the 
`FROM` command.
The Collection's constructor accepts a DBObject type to build the 
subsequent objects array from the table name.

Several functions can be executed effortlessly using Go code and loops. Therefore, 
Collection does not furnish all SQL operations: You can conveniently retrieve the 
underlying array via the `GetArray()` method and apply your logic on it.

Collection provides:

- **`Distinct`:** Removes all duplicates.
- **`Sort`:** Accepts a sorting function and arranges the collection accordingly.
- **`Filter`:** Accepts a predicate function and excludes all items that fail the condition.
- **`Where`:** Operates like a `JOIN` but uses the link concept of this library.

### CRUD Triggers

> Register functions that will be executed before or after CRUD operation for a specific
> table name.

In SQL, a trigger is a stored procedure that runs automatically when an event occurs in
the database server. In FluentKV, the concept of a 'trigger' is implemented through
`AddTrigger` and `DeleteTrigger` functions.

- **`AddAfterTrigger`:** Set up a trigger which fires either before or after a
  specified operation in the table. The trigger is represented by an `action` function
  which is automatically executed when the trigger fires. The `action` function is called
  with the id of the object and the object itself when it's available.
- **`AddBeforeTrigger`:** The trigger set by this function will be executed before the
  operation.
  This allows the current operation to be aborted by returning an error.
  If multiple triggers are registered before an operation, all will be executed, but if
  only one returns an error, the operation will be aborted.
- **`DeleteTrigger`:** Allows you to remove a previously added trigger from the
  database.
  All you need is to provide the id of the trigger to be deleted.

This feature provides a handy way to automatically manage and react to changes in the KV
database.

## Usage

> This section explains basic usage with preparation of a type and, first of usage and more
> advanced use case.
> You can also see the test sources for additional information.

### Prepare your types

For the first time, you can declare a type like this:

```go
type Person struct {
    DBObject // Note this, is for the default implementation.
    Firstname  string
    Lastname  string
    Age int
}
```

`IObject` has four operations (`Equals`, `Hash`, `ToString`, `TableName`), `DBObject` implement
`Equals` and `Hash`. You can override this implementation, but you need to implement `ToString`
and `TableName`. You can simply use the helpers like this:

```go
func (t Person) ToString() string  { return helper.ToString(t) }

func (t Person) TableName() string { return helper.NameOfStruct[Person]() }
```

To simplify the construction of `IObject` is recommended to make a constructor (it can
internally affect the `DBObject` with the new instance created):

```go
func NewPerson(firstname string, lastname string, age int) Person {
    person := Person{Firstname: firstname, Lastname: lastname, Age: age}
    person.IObject = person // Mandatory (here or manualy before using). 

    return person
}
```

In last time, you need to register your type to Gob (the binary serializer used) like
this (do it before use):

```go
gob.Register(Person{})
```

### Basic operations

```go
// Initialize db
AutoKeyBuffer = 10 // Optional tweaking
db, _ := NewBadgerDB("data")
```

- **Insert, Get, Set:** There is the basic operation to store and use data from the db.
  ```go
  person := NewPerson("Foo", "Bar", 42)
  personWrapped = Insert(db, person) // Insert a DBObject in db. 
  // This operation return an ObjWrapper[Person]
  
  // Get the Person object from the id:
  valueWrp := Get[Person](db, personWrapped.ID)
  
  // Change the value at the index personWrapped.ID:
  Set(db, NewPerson("Bar", "Foo", 1), personWrapped.ID)
  ```

- **Update:** Allows you to edit the saved object without recreating it.
  Useful if the object has many fields, and you want to edit only one for example.
  ```go
  Update(db, personWrapped.ID, func (person *Person) {
      person.Age = 12
  })
  ```

- **Exist, Count:**
  ```go
  Count[Person](db)                   // Count = 1
  Exist[Person](db, personWrapped.ID) // Exist = true
  ```

- **Link, LinkNew, UnlinkAll, RemoveLink, RemoveAllLink:**
  Link concept allows retrieving an object of another table linked to the current one.
  ```go
  // It supposed we have a struct Address(Street string, City string) for example.
  
  addressWrp := Insert(db, NewAddress("", ""))
  Link(addressWrp, true, personWrapped)
  // We can similarly do this:
  // addressWrp := LinkNew(NewAddress("", ""), true, personWrapped)
  
  AllFromLink[Person, Address](personWrapped) // Result is an array with only addressWrp.
  
  RemoveLink(addressWrp, personWrapped)
  ```

- **Delete, DeepDelete:**
  ```go
  addressWrp := Insert(db, NewAddress("", ""))
  Link(addressWrp, true, personWrapped)
  
  DeepDeleteWrp[Address](addressWrp) // Delete addressWrp and personWrapped.

  // Or for delete only addressWrp:
  // DeleteWrp[Address](addressWrp) 
  ```

- **FindFirst, FindAll:**
  ```go
  // Prepare a list of persons.
  persons := []Person{
      NewPerson("Jean", "Smith", 21),
      NewPerson("Richard", "Smith", 25),
      NewPerson("James", "Smith", 32),
  }
  
  // Fill the db with this new person.
  for _, person := range persons {
      Insert(db, person)
  }
  
  // Search the first person 25 years old (Richard):
  result := FindFirst(db, func(id string, person *Person) bool {
      return person.Age == 25
  })
  
  // Or search all persons more than 22 years old (Richard, James):
  results := FindAll(db, func(id string, person *Person) bool {
      return person.Age > 22
  })
  ```

- **Foreach, Visit:**
  ```go
  personWrp = Insert(db, NewPerson("Foo", "Bar", 42))
  addressWrp := Insert(db, NewAddress("", ""))

  Link(addressWrp, true, personWrp)
    
  // Foreach provide a way to perform some operation on a copy of each value of a table:
  Foreach(db, func(id string, value *Person) {
      // Use `id` and `value` here.
  })
  
  // Visit permit to retreive all ids of object linked to another one:
  ids := VisitWrp[Address, Person](addressWrp) 
  // len(ids) == 1 ; ids[0] == personWrp.ID
  ```

### Triggers

- **AddTrigger:**
  ```go
  // Add a trigger to log person inserted:
  _ = AddAfterTrigger(db, "log", InsertOperation, false, func(id string, person Person) {
      fmt.println("new person added:", person.Firstname)
  })
  
  // Add a trigger to multiple operation:
  _ = AddAfterTrigger(db, "my trigger", UpdateOperation | DeleteOperation, false,
          func(id string, person Person) {
              // Do something
          })

    // Add a trigger before an operation:
  _ = AddBeforeTrigger(db, "check", InsertOperation, false,
          func(id string, person Person) error {
              // Perform some check
			  return nil // Or return error to cancel current operation
          })
  ```

- **DeleteTrigger:**
  ```go
  _ = DeleteTrigger[Person](db, "log")
  ```

### [TODO] More advanced operations

