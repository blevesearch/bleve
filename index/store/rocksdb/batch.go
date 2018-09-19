package rocksdb

import (
	"github.com/tecbot/gorocksdb"
)

type Batch struct {
	batch *gorocksdb.WriteBatch
}

func (b *Batch) Set(key, val []byte) {
	b.batch.Put(key, val)
}

func (b *Batch) Delete(key []byte) {
	b.batch.Delete(key)
}

func (b *Batch) Merge(key, val []byte) {
	//b.batch.Merge(key, val)
}

func (b *Batch) Reset() {
	b.batch.Clear()
}

func (b *Batch) Close() error {
	b.batch.Destroy()
	b.batch = nil
	return nil
}
