package leveldb

import (
	"github.com/jmhodges/levigo"
)

type LevelDBBatch struct {
	store *LevelDBStore
	batch *levigo.WriteBatch
}

func newLevelDBBatch(store *LevelDBStore) *LevelDBBatch {
	rv := LevelDBBatch{
		store: store,
		batch: levigo.NewWriteBatch(),
	}
	return &rv
}

func (ldb *LevelDBBatch) Set(key, val []byte) {
	ldb.batch.Put(key, val)
}

func (ldb *LevelDBBatch) Delete(key []byte) {
	ldb.batch.Delete(key)
}

func (ldb *LevelDBBatch) Execute() error {
	return ldb.store.db.Write(defaultWriteOptions(), ldb.batch)
}

func (ldb *LevelDBBatch) Close() error {
	ldb.batch.Close()
	return nil
}
