package cockroach

import (
	"log"

	"github.com/blevesearch/bleve/index/store"
)

type reader struct {
	s *Store
}

func (r *reader) Get(key []byte) ([]byte, error) {
	return r.s.t.Get(key)
}

func (r *reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return r.s.t.MultiGet(keys)
}

func (r *reader) PrefixIterator(prefix []byte) store.KVIterator {
	keys, values, err := r.s.t.PrefixGet(prefix)
	if err != nil {
		log.Println(err) // nowhere where we can surface that error
	}
	return &iterator{
		keys:   keys,
		values: values,
	}
}

func (r *reader) RangeIterator(start, end []byte) store.KVIterator {
	keys, values, err := r.s.t.RangeGet(start, end)
	if err != nil {
		log.Println(err) // nowhere where we can surface that error
	}
	return &iterator{
		keys:   keys,
		values: values,
	}
}

func (r *reader) Close() error {
	r.s = nil
	return nil
}
