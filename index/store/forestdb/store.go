//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

package forestdb

import (
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/couchbaselabs/goforestdb"
)

const Name = "leveldb"

type Store struct {
	path   string
	config *forestdb.Config
	db     *forestdb.Database
	writer sync.Mutex
}

func Open(path string, createIfMissing bool) (*Store, error) {
	rv := Store{
		path:   path,
		config: forestdb.DefaultConfig(),
	}

	if createIfMissing {
		rv.config.SetOpenFlags(forestdb.OPEN_FLAG_CREATE)
	}

	var err error
	rv.db, err = forestdb.Open(rv.path, rv.config)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (s *Store) get(key []byte) ([]byte, error) {
	res, err := s.db.GetKV(key)
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return nil, err
	}
	return res, nil
}

func (s *Store) set(key, val []byte) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	return s.setlocked(key, val)
}

func (s *Store) setlocked(key, val []byte) error {
	return s.db.SetKV(key, val)
}

func (s *Store) delete(key []byte) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	return s.deletelocked(key)
}

func (s *Store) deletelocked(key []byte) error {
	return s.db.DeleteKV(key)
}

func (s *Store) commit() error {
	return s.db.Commit(forestdb.COMMIT_NORMAL)
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (ldbs *Store) iterator(key []byte) store.KVIterator {
	rv := newIterator(ldbs)
	rv.Seek(key)
	return rv
}

func (s *Store) Reader() (store.KVReader, error) {
	return newReader(s)
}

func (ldbs *Store) Writer() (store.KVWriter, error) {
	return newWriter(ldbs)
}

func (ldbs *Store) newBatch() store.KVBatch {
	return newBatch(ldbs)
}

func (s *Store) newSnapshot() (*forestdb.Database, error) {
	dbinfo, err := s.db.DbInfo()
	if err != nil {
		return nil, err
	}
	return s.db.SnapshotOpen(dbinfo.LastSeqNum())
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	createIfMissing := false
	cim, ok := config["create_if_missing"].(bool)
	if ok {
		createIfMissing = cim
	}
	return Open(path, createIfMissing)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}
