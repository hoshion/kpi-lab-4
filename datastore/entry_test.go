package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "value", nil}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{"key", "test-value", nil}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bat value [%s]", v)
	}
}

func TestCheckSum(t *testing.T) {
	e := entry{"key", "test-value", nil}
	data := e.Encode()
	newEntry := entry{}
	newEntry.Decode(data)

	if bytes.Compare(newEntry.sum, []byte{144, 131, 170, 173, 93, 163, 160, 73, 213, 155, 208, 224, 74, 82, 135, 153, 183, 128, 175, 214}) != 0 {
		t.Errorf("Check sum calculated incorrectly")
	}
}
