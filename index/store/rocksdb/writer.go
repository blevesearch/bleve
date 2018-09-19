
package rocksdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/tecbot/gorocksdb"
)

type Writer struct {
	store   *Store
	options *gorocksdb.WriteOptions
}

func (w *Writer) NewBatch() store.KVBatch {
	rv := Batch{
		batch: gorocksdb.NewWriteBatch(),
	}
	return &rv
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if ok {
		return w.store.db.Write(w.options, batch.batch)
	}
	return fmt.Errorf("wrong type of batch")
}

func (w *Writer) Close() error {
	//w.options.Destroy()
	return nil
}
