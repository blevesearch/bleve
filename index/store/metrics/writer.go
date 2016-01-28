package metrics

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	s *Store
	o store.KVWriter
}

func (w *Writer) Close() error {
	err := w.o.Close()
	if err != nil {
		w.s.AddError("Writer.Close", err, nil)
	}
	return err
}

func (w *Writer) NewBatch() store.KVBatch {
	return &Batch{s: w.s, o: w.o.NewBatch()}
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	buf, b, err := w.o.NewBatchEx(options)
	if err != nil {
		return nil, nil, err
	}
	return buf, &Batch{s: w.s, o: b}, nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) (err error) {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}
	w.s.TimerWriterExecuteBatch.Time(func() {
		err = w.o.ExecuteBatch(batch.o)
		if err != nil {
			w.s.AddError("Writer.ExecuteBatch", err, nil)
		}
	})
	return
}
