package helper_test

import (
	"github.com/Phosmachina/FluentKV/helper"
	"testing"
)

type SampleStruct struct{}
type AnotherStruct struct{}

func TestGetStructName(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{SampleStruct{}, "SampleStruct"},
		{&SampleStruct{}, "SampleStruct"},
		{AnotherStruct{}, "AnotherStruct"},
		{123, ""},    // Non-struct input
		{"test", ""}, // Non-struct input
	}

	for _, test := range tests {
		actual := helper.StructName(test.input)
		if actual != test.expected {
			t.Errorf("GetStructName(%v): expected '%s', got '%s'", test.input, test.expected, actual)
		}
	}
}
