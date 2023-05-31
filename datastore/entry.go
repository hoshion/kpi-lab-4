package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
)

type entry struct {
	key, value string
	sum        []byte
}

func getLength(key string, value string) int64 {
	return int64(len(key) + len(value) + 12)
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + 32
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	binary.LittleEndian.PutUint32(res[8:], uint32(vl))
	copy(res[12:], e.key)
	copy(res[kl+12:], e.value)
	data := make([]byte, size-20)
	copy(data, res[:size-19])
	sum := sha1.Sum(data)
	copy(res[size-20:], sum[:])

	return res
}

func (e *entry) getLength() int64 {
	return getLength(e.key, e.value)
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	vl := binary.LittleEndian.Uint32(input[8:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[12:kl+12])
	e.key = string(keyBuf)

	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)
	e.sum = make([]byte, 20)
	copy(e.sum, input[kl+vl+12:])
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(12)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	valSize := int(binary.LittleEndian.Uint32(header[8:]))

	data, err := in.Peek(12 + keySize + valSize)
	if err != nil {
		return "", err
	}

	_, err = in.Discard(12 + keySize)
	if err != nil {
		return "", err
	}

	valueData, err := in.Peek(valSize)
	if err != nil {
		return "", err
	}
	if len(valueData) != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", len(valueData), valSize)
	}

	_, err = in.Discard(valSize)
	if err != nil {
		return "", err
	}

	sum, err := in.Peek(20)
	if err != nil {
		return "", err
	}
	realSum := sha1.Sum(data)
	if bytes.Compare(sum, realSum[:]) != 0 {
		return "", errors.New("SHA1 Sum is incorrect")
	}

	return string(valueData), nil
}
