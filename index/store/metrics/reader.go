package metrics

import "github.com/blevesearch/bleve/index/store"

type Reader struct {
	s *Store
	o store.KVReader
}

func (r *Reader) Get(key []byte) (v []byte, err error) {
	r.s.timerReaderGet.Time(func() {
		v, err = r.o.Get(key)
		if err != nil {
			r.s.AddError("Reader.Get", err, key)
		}
	})
	return
}

func (r *Reader) MultiGet(keys [][]byte) (vals [][]byte, err error) {
	r.s.timerReaderMultiGet.Time(func() {
		vals, err = r.o.MultiGet(keys)
		if err != nil {
			r.s.AddError("Reader.MultiGet", err, nil)
		}
	})
	return
}

func (r *Reader) PrefixIterator(prefix []byte) (i store.KVIterator) {
	r.s.timerReaderPrefixIterator.Time(func() {
		i = &Iterator{s: r.s, o: r.o.PrefixIterator(prefix)}
	})
	return
}

func (r *Reader) RangeIterator(start, end []byte) (i store.KVIterator) {
	r.s.timerReaderRangeIterator.Time(func() {
		i = &Iterator{s: r.s, o: r.o.RangeIterator(start, end)}
	})
	return
}

func (r *Reader) Close() error {
	err := r.o.Close()
	if err != nil {
		r.s.AddError("Reader.Close", err, nil)
	}
	return err
}
