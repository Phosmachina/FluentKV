package main

import (
	. "git.haythor.ml/naxxar/relational-badger/db"
	. "git.haythor.ml/naxxar/relational-badger/reldb"
)

func main() {
	db, _ := NewBadgerDB("data")

	location := Object(Location{})
	locationWrapper := db.Insert(&location)
	locationWrapper.AllFromLink(Type[Location]().TableName())
	home := Object(Home{})
	_ = db.Insert(&home).Link(locationWrapper.ID)
}

//region Objects declaration

type Home struct {
	Id       string
	Name     string
	Location string
}

// region Home impl

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

type Location struct {
	Zip     string
	City    string
	Address string
}

// region Location impl

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
