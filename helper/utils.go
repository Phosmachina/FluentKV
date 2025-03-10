package helper

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

// StructName retrieves the name of the struct as a string from an `any` value.
func StructName(value any) string {
	t := reflect.TypeOf(value)

	// Handle if the input is a pointer
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() == reflect.Struct {
		return t.Name()
	}

	return ""
}

func ToString(v any) string {

	// Handle nil value
	if v == nil {
		return "<nil>"
	}

	// Get the type and value of 'v'
	value := reflect.ValueOf(v)
	typeOf := reflect.TypeOf(v)

	// If 'v' is a pointer, dereference it
	if typeOf.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "<nil pointer>"
		}
		value = value.Elem()
		typeOf = typeOf.Elem()
	}

	// Ensure we are dealing with a struct
	if typeOf.Kind() != reflect.Struct {
		return "unsupported type"
	}

	// Start building the result string
	result := typeOf.Name()
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		fieldValue := value.Field(i)
		result += fmt.Sprintf(" | %s: %v", field.Name, fieldValue.Interface())
	}

	return result
}

func Hash(v any) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(ToString(v))))
}

func Equals(v1 any, v2 any) bool { return Hash(v1) == Hash(v2) }

// IndexOf returns the index of the first occurrence of an element in the provided slice,
// or -1 if any element is not present in the slice.
func IndexOf[T comparable](element T, data []T) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 // not found.
}

// Remove the item from the given slice.
// Return false when the element was not in the slice,
// true when it is successfully removed.
func Remove[T comparable](slice *[]T, item T) bool {

	for i, value := range *slice {
		if value == item {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return true
		}
	}

	return false
}

func RemoveWithPredicate[T any](slice *[]T, item T, predicate func(T, T) bool) bool {

	for i, value := range *slice {
		if predicate(value, item) {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return true
		}
	}

	return false
}

// Name returns the full name of the given function using reflection.
func Name(f func(*testing.T)) string {
	pc := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
	return pc.Name()
}
