package inmem

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/ryszard/goskiplist/skiplist"
)

type InMemStore struct {
	list *skiplist.SkipList
}

func Open() (*InMemStore, error) {
	rv := InMemStore{
		list: skiplist.NewStringMap(),
	}

	return &rv, nil
}

func MustOpen() *InMemStore {
	rv := InMemStore{
		list: skiplist.NewStringMap(),
	}

	return &rv
}

func (i *InMemStore) Get(key []byte) ([]byte, error) {
	val, ok := i.list.Get(string(key))
	if ok {
		return []byte(val.(string)), nil
	}
	return nil, nil
}

func (i *InMemStore) Set(key, val []byte) error {
	i.list.Set(string(key), string(val))
	return nil
}

func (i *InMemStore) Delete(key []byte) error {
	i.list.Delete(string(key))
	return nil
}

func (i *InMemStore) Commit() error {
	return nil
}

func (i *InMemStore) Close() error {
	return nil
}

func (i *InMemStore) Iterator(key []byte) store.KVIterator {
	rv := newInMemIterator(i)
	rv.Seek(key)
	return rv
}

func (i *InMemStore) NewBatch() store.KVBatch {
	return newInMemBatch(i)
}
