package leveldb

import (
	"github.com/jmhodges/levigo"
)

type LevelDBIterator struct {
	store    *LevelDBStore
	iterator *levigo.Iterator
}

func newLevelDBIterator(store *LevelDBStore) *LevelDBIterator {
	rv := LevelDBIterator{
		store:    store,
		iterator: store.db.NewIterator(defaultReadOptions()),
	}
	return &rv
}

func (ldi *LevelDBIterator) SeekFirst() {
	ldi.iterator.SeekToFirst()
}

func (ldi *LevelDBIterator) Seek(key []byte) {
	ldi.iterator.Seek(key)
}

func (ldi *LevelDBIterator) Next() {
	ldi.iterator.Next()
}

func (ldi *LevelDBIterator) Current() ([]byte, []byte, bool) {
	if ldi.Valid() {
		return ldi.Key(), ldi.Value(), true
	}
	return nil, nil, false
}

func (ldi *LevelDBIterator) Key() []byte {
	return ldi.iterator.Key()
}

func (ldi *LevelDBIterator) Value() []byte {
	return ldi.iterator.Value()
}

func (ldi *LevelDBIterator) Valid() bool {
	return ldi.iterator.Valid()
}

func (ldi *LevelDBIterator) Close() {
	ldi.iterator.Close()
}
