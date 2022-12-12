package main

import (
	. "relational-badger/db"
	. "relational-badger/reldb"
)

func main() {
	db, _ := NewBadgerDB("data")

	location := Object(Location{})
	locationWrapper := db.Insert(&location)
	locationWrapper.AllFromLink(Type[Location]().TableName())
	home := Object(Home{})
	_ = db.Insert(&home).Link(locationWrapper.ID)
}

//region Object declaration

// region Home

type Home struct {
	Id       string
	Name     string
	Location string
}

func (h Home) TableName() string {
	return NameOfStruct[Home]()
}

func (h Home) Hash() string {
	//TODO implement me
	panic("implement me")
}

func (h Home) ToString() string {
	//TODO implement me
	panic("implement me")
}

//endregion

// region Location

type Location struct {
	Zip     string
	City    string
	Address string
}

func (l Location) TableName() string {
	return NameOfStruct[Location]()
}

func (l Location) Hash() string {
	//TODO implement me
	panic("implement me")
}

func (l Location) ToString() string {
	//TODO implement me
	panic("implement me")
}

//endregion

//endregion
