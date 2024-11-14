package core

import (
	"bytes"
	"encoding/gob"
	"log"
)

func Encode(obj *IObject) []byte {

	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(obj)
	if err != nil {
		// TODO return err ; make some custom err
		log.Printf(err.Error())
		return nil
	}
	return buffer.Bytes()
}

func Decode(value []byte) *IObject {

	buffer := bytes.Buffer{}
	var object *IObject
	buffer.Write(value)
	err := gob.NewDecoder(&buffer).Decode(&object)
	if err != nil {
		return nil
		// TODO return nil/err ; make some custom err
	}

	return object
}
