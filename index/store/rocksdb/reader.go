package rocksdb

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/tecbot/gorocksdb"
)

type Reader struct {
	store    *Store
	snapshot *gorocksdb.Snapshot
	options  *gorocksdb.ReadOptions
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	return r.store.db.GetBytes(r.options, key)
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	opt := r.store.newReadOptions()
	opt.SetFillCache(false)
	opt.SetSnapshot(r.snapshot)
	rv := Iterator{
		option:   opt,
		iterator: r.store.db.NewIterator(opt),
		prefix:   prefix,
	}
	rv.Seek(prefix)
	return &rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	opt := r.store.newReadOptions()
	opt.SetFillCache(false)
	opt.SetSnapshot(r.snapshot)
	rv := Iterator{
		option:   opt,
		iterator: r.store.db.NewIterator(opt),
		start:    start,
		end:      end,
	}
	rv.Seek(start)
	return &rv
}

func (r *Reader) Close() error {
	r.options.Destroy()
	r.store.db.ReleaseSnapshot(r.snapshot)
	return nil
}
