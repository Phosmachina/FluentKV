
<h1 align="center">
    <img src="docs/FluentKV_banner.svg" alt="Banner" style="text-align: center; width: 70%">
</h1>

<div align="center">

[![GoDoc](https://godoc.org/github.com/Phosmachina/FluentKV?status.svg)](https://pkg.go.dev/github.com/Phosmachina/FluentKV#section-documentation)
[![Go Report Card](https://goreportcard.com/badge/github.com/Phosmachina/FluentKV)](https://goreportcard.com/badge/github.com/Phosmachina/FluentKV)

</div>

> Purpose a fluent toolkit for using a KV database.

## System Architecture

### Interface and Abstract Struct Implementation

The interface **IRelationalDB** defines a set of raw operations that must be implemented. It also holds some additional simpler operations already realized but they can be overridden if required. For further details, refer to the file `reldb/relational.go`.

The abstract struct **AbstractRelDB** extends **IRelationalDB**. It pre-implements the key filler system and all other operations using raw operations provided by the implementation. Check the file `reldb/abstract.go` for a comprehensive understanding.

### IObject Interface and ObjWrapper Struct

The `IObject` interface contains operations essential for this architecture. These operations, such as `Equals`, `Hash`, are primarily implemented with the `DBObject` abstraction but the `TableName` function needs explicit implementation. This function generates a key corresponding to data.

The `ObjWrapper` struct is a helper to store a value along with its unique id. It is used extensively across this toolkit to bind the IObject with the database or to return the value with its corresponding id.

### Implementation

At present, there is one main implementation for a KV Database:

- [BadgerDB](https://github.com/dgraph-io/badger)
- [Redis](https://redis.io) (This is currently being considered)

### Collection

> A straightforward way of performing operations on a list of items provided by a TableName.

In SQL, certain operations are performed on a list of rows, usually fetched using the `FROM` command. The Collection's constructor accepts a DBObject type to build the subsequent objects array from the TableName.

Several functions can be executed effortlessly using Go code and loops. Therefore, Collection doesn’t furnish all SQL operations: You can conveniently retrieve the underlying array via the `GetArray()` method and apply your logic on it.

Collection provides:

- **`Distinct`:** Removes all duplicates.
- **`Sort`:** Accepts a sorting function and arranges the collection accordingly.
- **`Filter`:** Accepts a predicate function and excludes all items that fail the condition.
- **`Where`:** Operates like a `JOIN` but uses the link concept of this library.

### [TODO] Handlers

> Register functions that will be executed on some event like **Insert**, **Delete** or **Update**.

## Usage

> This section explain basic usage with preparation of type and, first of usage and more
> advanced use case. You can also see the tests sources for additional information.

### Prepare your types

In first time, you can declare a type like this:

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
func (t Person) ToString() string  { return reldb.ToString(t) }

func (t Person) TableName() string { return reldb.NameOfStruct[Person]() }
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

In last time, you need to register your type to Gob (the binary serializer used) like this (simply
do it before use):

```go
gob.Register(Person{})
```

### Basic operations

```go
// Initialize db
AutoKeyBuffer = 10
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
  Useful if the object has many fields and you want to edit only one for example.
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
  Link concept allows to retrieve an object of another table linked to the current one.
  ```go
  // It supposed we have a struct Address(Street string, City string) for example.
  
  addressWrp := Insert(db, NewAddress("", ""))
  Link(addressWrp, true, personWrapped)
  // We can do this instead of:
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
  results := FindFirst(db, func(id string, person *Person) bool {
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

### [TODO] More advanced operations

