package goforestdb

import (
	"github.com/couchbaselabs/goforestdb"
)

type ForestDBIterator struct {
	store *ForestDBStore
	valid bool
	curr  *forestdb.Doc
	iter  *forestdb.Iterator
}

func newForestDBIterator(store *ForestDBStore) *ForestDBIterator {
	rv := ForestDBIterator{
		store: store,
	}
	return &rv
}

func (f *ForestDBIterator) SeekFirst() {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.store.db.IteratorInit([]byte{}, nil, forestdb.ITR_NONE)
	if err != nil {
		f.valid = false
		return
	}
	f.valid = true
	f.Next()
}

func (f *ForestDBIterator) Seek(key []byte) {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.store.db.IteratorInit(key, nil, forestdb.ITR_NONE)
	if err != nil {
		f.valid = false
		return
	}
	f.valid = true
	f.Next()
}

func (f *ForestDBIterator) Next() {
	var err error
	f.curr, err = f.iter.Next()
	if err != nil {
		f.valid = false
	}
}

func (f *ForestDBIterator) Current() ([]byte, []byte, bool) {
	if f.valid {
		return f.Key(), f.Value(), true
	}
	return nil, nil, false
}

func (f *ForestDBIterator) Key() []byte {
	if f.valid && f.curr != nil {
		return f.curr.Key()
	}
	return nil
}

func (f *ForestDBIterator) Value() []byte {
	if f.valid && f.curr != nil {
		return f.curr.Body()
	}
	return nil
}

func (f *ForestDBIterator) Valid() bool {
	return f.valid
}

func (f *ForestDBIterator) Close() {
	f.valid = false
	f.iter.Close()
	f.iter = nil
}
