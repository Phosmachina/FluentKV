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
gob.Register(Person{})

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

```


- **FindFirst, FindAll:**
```go

```

- **Visit, Foreach:**
```go

```

### More advanced operations

