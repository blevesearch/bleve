package metrics

import "github.com/blevesearch/bleve/index/store"

type Iterator struct {
	s *Store
	o store.KVIterator
}

func (i *Iterator) Seek(x []byte) {
	i.s.TimerIteratorSeek.Time(func() {
		i.o.Seek(x)
	})
}

func (i *Iterator) Next() {
	i.s.TimerIteratorNext.Time(func() {
		i.o.Next()
	})
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.o.Current()
}

func (i *Iterator) Key() []byte {
	return i.o.Key()
}

func (i *Iterator) Value() []byte {
	return i.o.Value()
}

func (i *Iterator) Valid() bool {
	return i.o.Valid()
}

func (i *Iterator) Close() error {
	err := i.o.Close()
	if err != nil {
		i.s.AddError("Iterator.Close", err, nil)
	}
	return err
}
