package cockroach

import (
	"bytes"
	"sort"
)

type iterator struct {
	keys   [][]byte
	values [][]byte
	index  int

	// value set by Seek* methods
	seek []byte

	currentK      []byte
	currentV      []byte
	currentOK     bool
	currentLoaded bool
}

func (i *iterator) SeekFirst() {
	i.index = 0
}

func (i *iterator) Seek(k []byte) {
	i.index = sort.Search(len(i.keys), func(idx int) bool {
		return bytes.Compare(i.keys[idx], k) >= 0
	})
}

func (i *iterator) Next() {
	i.index++
}

func (i *iterator) Current() ([]byte, []byte, bool) {
	if i.index < len(i.keys) {
		return i.keys[i.index], i.values[i.index], true
	}
	return nil, nil, false
}

func (i *iterator) Key() []byte {
	if i.index < len(i.keys) {
		return i.keys[i.index]
	}
	return nil
}

func (i *iterator) Value() []byte {
	if i.index < len(i.keys) {
		return i.values[i.index]
	}
	return nil
}

func (i *iterator) Valid() bool {
	return i.index < len(i.keys)
}

func (i *iterator) Close() error {
	i.keys = nil
	i.values = nil
	return nil
}
