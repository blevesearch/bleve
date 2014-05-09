package leveldb

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/jmhodges/levigo"
)

type LevelDBStore struct {
	path string
	opts *levigo.Options
	db   *levigo.DB
}

func Open(path string) (*LevelDBStore, error) {
	rv := LevelDBStore{
		path: path,
	}

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	rv.opts = opts

	var err error
	rv.db, err = levigo.Open(rv.path, rv.opts)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (ldbs *LevelDBStore) Get(key []byte) ([]byte, error) {
	return ldbs.db.Get(defaultReadOptions(), key)
}

func (ldbs *LevelDBStore) Set(key, val []byte) error {
	return ldbs.db.Put(defaultWriteOptions(), key, val)
}

func (ldbs *LevelDBStore) Delete(key []byte) error {
	return ldbs.db.Delete(defaultWriteOptions(), key)
}

func (ldbs *LevelDBStore) Commit() error {
	return nil
}

func (ldbs *LevelDBStore) Close() error {
	ldbs.db.Close()
	return nil
}

func (ldbs *LevelDBStore) Iterator(key []byte) store.KVIterator {
	rv := newLevelDBIterator(ldbs)
	rv.Seek(key)
	return rv
}

func (ldbs *LevelDBStore) NewBatch() store.KVBatch {
	return newLevelDBBatch(ldbs)
}
