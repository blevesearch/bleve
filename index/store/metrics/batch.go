package metrics

import "github.com/blevesearch/bleve/index/store"

type Batch struct {
	s *Store
	o store.KVBatch
}

func (b *Batch) Set(key, val []byte) {
	b.o.Set(key, val)
}

func (b *Batch) Delete(key []byte) {
	b.o.Delete(key)
}

func (b *Batch) Merge(key, val []byte) {
	b.s.TimerBatchMerge.Time(func() {
		b.o.Merge(key, val)
	})
}

func (b *Batch) Reset() {
	b.o.Reset()
}

func (b *Batch) Close() error {
	err := b.o.Close()
	b.o = nil
	return err
}
