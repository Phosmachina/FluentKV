# FluentKV

> Purpose a fluent toolkit for using a KV storage.

## Architecture

### Interface and abstraction

### IObject and ObjWrapper

### Implementation

### Collection

> This purpose a simple way to make some operation on a list items provided by a TableName.

In SQL, you have some operation applied on a list of row. List of row are, in main case,
provided with `FROM` command. The constructor of Collection take a DBObject type to build the
corresponding objects array with the TableName.

Many operations can be simply done with a Go code and loop. Because of that Collection not 
provide all SQL operations: you can easily get the underlying array with the `GetArray()` 
functions and make your logic on it.

Collection provide:
- **`Distinct`:** Eliminate all duplicate.
- **`Sort`:** Take the logic of sorting and sort the collection.
- **`Filter`:** Take a predicate function and eliminate all not valid items.
- **`Where`:** Like a `JOIN` but use the link concept used here.

### Handlers

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

- **Insert, Get, Set:**
- **Update:**
- **Delete, DeepDelete:**

- **Exist, Count:**
- **FindFirst, FindAll:**

### More advanced operations

