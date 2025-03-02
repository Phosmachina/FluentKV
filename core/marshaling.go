package core

import (
	"bytes"
	"encoding/gob"
	"errors"
)

// TODO make an interface to allow marshaling customisations.

var (
	EncodeErr = errors.New("encode failed")
	DecodeErr = errors.New("decode failed")
)

type IMarshaller interface {
	Encode(*any) ([]byte, error)
	Decode([]byte) (*any, error)
}

type GobMarshaller struct{}

func (g *GobMarshaller) Encode(value *any) ([]byte, error) {

	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(value)
	if err != nil {
		return nil, EncodeErr
	}

	return buffer.Bytes(), nil
}

func (g *GobMarshaller) Decode(value []byte) (*any, error) {

	buffer := bytes.Buffer{}
	buffer.Write(value)

	var object any
	err := gob.NewDecoder(&buffer).Decode(&object)
	if err != nil {
		return nil, DecodeErr
	}

	return &object, nil
}
