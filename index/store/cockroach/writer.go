package cockroach

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
)

type writer struct {
	s *Store
}

func (w *writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.s.mo)
}

func (w *writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *writer) ExecuteBatch(batch store.KVBatch) error {
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)
		existingVal, err := w.s.t.Get(kb)
		if err != nil {
			return fmt.Errorf("ExecuteBatch.Get: %v", err)
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		err = w.s.t.Upsert(kb, mergedVal)
		if err != nil {
			return fmt.Errorf("ExecuteBatch.Upsert: %v", err)
		}
	}

	var err error
	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			err = w.s.t.Upsert(op.K, op.V)
		} else {
			err = w.s.t.Delete(op.K)
		}
		if err != nil {
			break
		}
	}

	return err
}

func (w *writer) Close() error {
	w.s = nil
	return nil
}
