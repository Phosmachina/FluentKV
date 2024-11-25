package helper

import (
	"fmt"
	"reflect"
)

// NameOfStruct simply reflect the name of the type T.
func NameOfStruct[T any]() string {
	return reflect.TypeOf((*T)(nil)).Elem().Name()
}

// ToString prints the name of type and all field names with the corresponding value.
func ToString(v any) string {

	typeOf := reflect.TypeOf(v)
	result := typeOf.Name()
	value := reflect.ValueOf(v)

	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		result += fmt.Sprintf(" | %s: %v", field.Name, value.Field(i))
	}

	return result
}

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

func Remove[T comparable](slice []T, item T) []T {
	for i, value := range slice {
		if value == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
