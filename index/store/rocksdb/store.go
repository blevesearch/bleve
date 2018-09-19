package rocksdb

import (
	"fmt"
	"os"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/tecbot/gorocksdb"
)

const Name = "rocksdb"

type Store struct {
	path   string
	opts   *gorocksdb.Options
	wOpt   *gorocksdb.WriteOptions
	config map[string]interface{}
	db     *gorocksdb.DB

	roptVerifyChecksums    bool
	roptVerifyChecksumsUse bool
	roptFillCache          bool
	roptFillCacheUse       bool
	roptReadTier           int
	roptReadTierUse        bool

	woptSync          bool
	woptSyncUse       bool
	woptDisableWAL    bool
	woptDisableWALUse bool
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	fmt.Println("config ", config)
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

	rv := Store{
		path:   path,
		config: config,
		opts:   gorocksdb.NewDefaultOptions(),
	}

	if mo != nil {
		rv.opts.SetMergeOperator(mo)
	}
	_, err := applyConfig(rv.opts, config)
	if err != nil {
		return nil, err
	}

	b, ok := config["read_only"].(bool)
	if ok && b {
		rv.db, err = gorocksdb.OpenDbForReadOnly(rv.opts, rv.path, false)
	} else {
		rv.db, err = gorocksdb.OpenDb(rv.opts, rv.path)
	}

	if err != nil {
		return nil, err
	}

	b, ok = config["readoptions_verify_checksum"].(bool)
	if ok {
		rv.roptVerifyChecksums, rv.roptVerifyChecksumsUse = b, true
	}

	b, ok = config["readoptions_fill_cache"].(bool)
	if ok {
		rv.roptFillCache, rv.roptFillCacheUse = b, true
	}

	v, ok := config["readoptions_read_tier"].(float64)
	if ok {
		rv.roptReadTier, rv.roptReadTierUse = int(v), true
	}

	b, ok = config["writeoptions_sync"].(bool)
	if ok {
		rv.woptSync, rv.woptSyncUse = b, true
	}

	b, ok = config["writeoptions_disable_WAL"].(bool)
	if ok {
		rv.woptDisableWAL, rv.woptDisableWALUse = b, true
	}
	rv.wOpt = rv.newWriteOptions()
	return &rv, nil
}

func (s *Store) Flush() error {
	return s.db.Flush(gorocksdb.NewDefaultFlushOptions())
}

func (s *Store) Close() error {
	s.opts.Destroy()
	s.wOpt.Destroy()
	s.opts = nil
	s.wOpt = nil

	s.db.Close()
	s.db = nil
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	snapshot := s.db.NewSnapshot()
	options := s.newReadOptions()
	options.SetSnapshot(snapshot)
	return &Reader{
		store:    s,
		snapshot: snapshot,
		options:  options,
	}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store:   s,
		options: s.wOpt,
	}, nil
}

func (s *Store) Compact() error {
	s.db.CompactRange(gorocksdb.Range{})
	return nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
