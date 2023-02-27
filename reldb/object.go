package reldb

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

func NewObjWrapper[T IObject](db IRelationalDB, ID string, value *T) *ObjWrapper[T] {
	return &ObjWrapper[T]{db: db, ID: ID, Value: *value}
}

// Link add a link between s object and all t objects.
// The biDirectional attribute determine if for t is also connected to s:
//
//		 biDirectional == false: s -> t
//
//		 biDirectional == true:  s -> t
//	                          s <- t
func Link[S IObject, T IObject](s *ObjWrapper[S], biDirectional bool, t ...*ObjWrapper[T]) {
	var q T
	tn := q.TableName()
	for _, v := range t {
		exist := s.db.Exist(tn, v.ID)
		if !exist {
			golog.Warnf("Id '%s' not found and cannot be link.", v.ID)
			continue
		}
		k := MakeLinkKey(s.Value.TableName(), s.ID, tn, v.ID)
		if biDirectional {
			s.db.RawSet(PrefixLink, k[1], nil)
		}
		s.db.RawSet(PrefixLink, k[0], nil)
	}
}

// LinkNew same as Link but take IObject array and insert them in the db then return the resulting
// wrapping.
func LinkNew[S IObject, T IObject](s *ObjWrapper[S], biDirectional bool, objs ...T) []*ObjWrapper[T] {
	var objsWrapped []*ObjWrapper[T]
	for _, obj := range objs {
		id := s.db.Insert(obj)
		wrapper := NewObjWrapper(s.db, id, &obj)
		Link[S, T](s, biDirectional, wrapper)
		objsWrapped = append(objsWrapped, wrapper)
	}
	return objsWrapped
}

// UnlinkAll returned all object, with the tableName induced by T, connected to the s object.
func UnlinkAll[S IObject, T IObject](s *ObjWrapper[S]) []*ObjWrapper[T] {
	var t T
	tn := t.TableName()
	var results []*ObjWrapper[T]
	s.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, s.Value.TableName()+Delimiter+s.ID+LinkDelimiter+tn) {
			value := (*s.db.Get(
				tn,
				strings.Split(strings.Split(key, LinkDelimiter)[1], Delimiter)[1])).(T)
			results = append(results, NewObjWrapper[T](s.db, key, &value))
		}
		return false
	})
	return results
}

// RemoveLink remove all link between s and t object. Return true if the link s->t are deleted (is
// at least the link created when isBidirectional == false).
func RemoveLink[S IObject, T IObject](s *ObjWrapper[S], t *ObjWrapper[T]) bool {
	k := MakeLinkKey(s.Value.TableName(), s.ID, t.Value.TableName(), t.ID)
	t.db.RawDelete(PrefixLink, k[1])
	return t.db.RawDelete(PrefixLink, k[0])
}

// RemoveAllTableLink remove all link between t object and object having the S tableName.
func RemoveAllTableLink[S IObject, T IObject](t *ObjWrapper[S]) {
	var q T
	tn := q.TableName()
	t.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		split := strings.Split(key, LinkDelimiter)
		tnAndIdL := strings.Split(split[0], Delimiter)
		tnAndIdR := strings.Split(split[1], Delimiter)

		if (tnAndIdL[0] == t.Value.TableName() && tnAndIdL[1] == t.ID && tnAndIdR[0] == tn) ||
			(tnAndIdR[0] == t.Value.TableName() && tnAndIdR[1] == t.ID && tnAndIdL[0] == tn) {
			t.db.RawDelete(PrefixLink, key)
		}
		return false
	})
}

// RemoveAllLink remove all link connected to this object.
func (t *ObjWrapper[T]) RemoveAllLink() {
	t.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		for _, s := range strings.Split(key, LinkDelimiter) {
			tnAndId := strings.Split(s, Delimiter)
			if tnAndId[1] == t.ID {
				t.db.RawDelete(PrefixLink, key)
			}
		}
		return false
	})
}

// Visit iterate on all connected objects and returns all ids.
func (t *ObjWrapper[IObject]) Visit(tableName string) []string {
	var resultIds []string
	t.db.RawIterKey(PrefixLink, func(key string) (stop bool) {
		if strings.HasPrefix(key, t.Value.TableName()+Delimiter+t.ID+LinkDelimiter+tableName) {
			resultIds = append(resultIds, key)
		}
		return false
	})
	return resultIds
}
