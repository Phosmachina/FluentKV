package tests

import (
	"bytes"
	"encoding/gob"
	stdjson "encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
)

type TestStruct struct {
	Name string
	Age  int
}

func BenchmarkGob(b *testing.B) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	dec := gob.NewDecoder(&buf)

	t := TestStruct{"John", 30}
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(&t); err != nil {
			b.Fatal(err)
		}
		t = TestStruct{}
		if err := dec.Decode(&t); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStdJSON(b *testing.B) {
	t := TestStruct{"John", 30}
	for i := 0; i < b.N; i++ {
		jsonBytes, err := stdjson.Marshal(&t)
		if err != nil {
			b.Fatal(err)
		}

		t = TestStruct{}
		err = stdjson.Unmarshal(jsonBytes, &t)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJsoniter(b *testing.B) {
	var json = jsoniter.ConfigFastest
	t := TestStruct{"John", 30}
	for i := 0; i < b.N; i++ {
		jsonBytes, err := json.Marshal(&t)
		if err != nil {
			b.Fatal(err)
		}

		t = TestStruct{}
		err = json.Unmarshal(jsonBytes, &t)
		if err != nil {
			b.Fatal(err)
		}
	}
}
