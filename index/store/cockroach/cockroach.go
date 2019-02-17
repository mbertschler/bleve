package cockroach

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const Name = "cockroach"

type Store struct {
	t  *table
	mo store.MergeOperator
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	table, err := openTable()
	if err != nil {
		return nil, err
	}

	store := &Store{
		t:  table,
		mo: mo,
	}

	return store, nil
}

func (s *Store) Close() error {
	return s.t.db.Close()
}

func (s *Store) Reader() (store.KVReader, error) {
	return &reader{s: s}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &writer{s: s}, nil
}

// I guess that is used somewhere, but optional

// type batch struct{}

// func (i *batch) Set(key, val []byte)   {}
// func (i *batch) Delete(key []byte)     {}
// func (i *batch) Merge(key, val []byte) {}
// func (i *batch) Reset()                {}
// func (i *batch) Close() error          { return nil }

func init() {
	registry.RegisterKVStore(Name, New)
}
