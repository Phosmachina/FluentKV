package fluentkv

import (
	"crypto/md5"
	"fmt"
	"github.com/kataras/golog"
	"strings"
)

type IObject interface {
	Equals(v IObject) bool
	Hash() string
	ToString() string
	TableName() string
}

type DBObject struct{ IObject }

func (o DBObject) Equals(v IObject) bool {
	return o.Hash() == v.Hash()
}

func (o DBObject) Hash() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(o.ToString())))
}

type ObjWrapper[T IObject] struct {
	db    IRelationalDB
	ID    string
	Value T
}

func (w *ObjWrapper[T]) Unwrap() (string, T) {
	return w.ID, w.Value
}

func NewObjWrapper[T IObject](db IRelationalDB, ID string, value *T) *ObjWrapper[T] {
	return &ObjWrapper[T]{db: db, ID: ID, Value: *value}
}

// Link add a link between s object and all targets objects.
// The biDirectional attribute determine if for target is also connected to s:
//
//		 biDirectional == false: s -> t
//
//		 biDirectional == true:  s -> t
//	                          s <- t
func Link[S IObject, T IObject](s *ObjWrapper[S], biDirectional bool, targets ...*ObjWrapper[T]) {

	TName := tableName[T]()

	for _, v := range targets {
		exist := s.db.Exist(TName, v.ID)
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", v.ID)
			continue
		}
		k := MakeLinkKey(s.Value.TableName(), s.ID, TName, v.ID)
		if biDirectional {
			s.db.RawSet(PrefixLink, k[1], nil)
		}
		s.db.RawSet(PrefixLink, k[0], nil)
	}
}

// LinkNew same as Link but take IObject array and insert them in the db then return the resulting
// wrapping.
func LinkNew[S IObject, T IObject](s *ObjWrapper[S], biDirectional bool, objs ...T) []*ObjWrapper[T] {

	var objsWrp []*ObjWrapper[T]

	for _, obj := range objs {
		id := s.db.Insert(obj)
		wrapper := NewObjWrapper(s.db, id, &obj)
		Link[S, T](s, biDirectional, wrapper)
		objsWrp = append(objsWrp, wrapper)
	}
	return objsWrp
}

// TODO potentially make FromLink method

// AllFromLink returned all object, with the tableName induced by T, connected to the S object.
func AllFromLink[S IObject, T IObject](db IRelationalDB, idOfS string) []*ObjWrapper[T] {

	var results []*ObjWrapper[T]
	SName := tableName[S]()
	TName := tableName[T]()

	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, SName+Delimiter+idOfS+LinkDelimiter+TName) {
			value := (*db.Get(
				TName,
				strings.Split(strings.Split(key, LinkDelimiter)[1], Delimiter)[1])).(T)
			results = append(results, NewObjWrapper[T](db, key, &value))
		}
		return false
	})
	return results
}

// AllFromLinkWrp returned all object, with the tableName induced by T, connected to the S object.
func AllFromLinkWrp[S IObject, T IObject](s *ObjWrapper[S]) []*ObjWrapper[T] {
	return AllFromLink[S, T](s.db, s.ID)
}

// RemoveLink remove all link between s and t object. Return true if the link s->t are deleted (is
// at least the link created when isBidirectional == false).
func RemoveLink[S IObject, T IObject](db IRelationalDB, idOfS string, idOfT string) bool {

	SName := tableName[S]()
	TName := tableName[T]()

	k := MakeLinkKey(SName, idOfS, TName, idOfT)
	db.RawDelete(PrefixLink, k[1])

	return db.RawDelete(PrefixLink, k[0])
}

// RemoveLinkWrp remove all link between s and t object. Return true if the link s->t are deleted (is
// at least the link created when isBidirectional == false).
func RemoveLinkWrp[S IObject, T IObject](s *ObjWrapper[S], t *ObjWrapper[T]) bool {
	return RemoveLink[S, T](s.db, s.ID, t.ID)
}

// RemoveAllTableLink remove all link between t object and object having the S tableName.
func RemoveAllTableLink[S IObject, T IObject](db IRelationalDB, id string) {

	SName := tableName[S]()
	TName := tableName[T]()

	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		split := strings.Split(key, LinkDelimiter)
		tnAndIdL := strings.Split(split[0], Delimiter)
		tnAndIdR := strings.Split(split[1], Delimiter)

		if (tnAndIdL[0] == SName && tnAndIdL[1] == id && tnAndIdR[0] == TName) ||
			(tnAndIdR[0] == SName && tnAndIdR[1] == id && tnAndIdL[0] == TName) {
			db.RawDelete(PrefixLink, key)
		}
		return false
	})
}

// RemoveAllTableLinkWrp remove all link between t object and object having the S tableName.
func RemoveAllTableLinkWrp[S IObject, T IObject](s *ObjWrapper[S]) {
	RemoveAllTableLink[S, T](s.db, s.ID)
}

// RemoveAllLink remove all link connected to this object.
func RemoveAllLink[T IObject](db IRelationalDB, id string) {

	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		for _, s := range strings.Split(key, LinkDelimiter) {
			tnAndId := strings.Split(s, Delimiter)
			if tnAndId[1] == id {
				db.RawDelete(PrefixLink, key)
			}
		}
		return false
	})
}

// RemoveAllLinkWrp remove all link connected to this object.
func RemoveAllLinkWrp[T IObject](t *ObjWrapper[T]) {
	RemoveAllLink[T](t.db, t.ID)
}

// TODO make the Visit method test

// Visit iterate on all connected objects and returns all ids. Prevent the value recovering.
func Visit[S IObject, T IObject](db IRelationalDB, id string) []string {

	var resultIds []string
	SName := tableName[S]()
	TName := tableName[T]()

	db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, TName+Delimiter+id+LinkDelimiter+SName) {
			resultIds = append(resultIds, key)
		}
		return false
	})

	return resultIds
}

// VisitWrp iterate on all connected objects and returns all ids. Prevent the value recovering.
func VisitWrp[S IObject, T IObject](s *ObjWrapper[S]) []string {
	return Visit[S, T](s.db, s.ID)
}
