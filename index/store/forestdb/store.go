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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/couchbaselabs/goforestdb"
)

const Name = "leveldb"

type Store struct {
	path     string
	config   *forestdb.Config
	kvconfig *forestdb.KVStoreConfig
	dbfile   *forestdb.File
	dbkv     *forestdb.KVStore
	writer   sync.Mutex
}

func Open(path string, createIfMissing bool) (*Store, error) {
	rv := Store{
		path:     path,
		config:   forestdb.DefaultConfig(),
		kvconfig: forestdb.DefaultKVStoreConfig(),
	}

	if createIfMissing {
		rv.kvconfig.SetCreateIfMissing(true)
	}

	var err error
	rv.dbfile, err = forestdb.Open(rv.path, rv.config)
	if err != nil {
		return nil, err
	}

	rv.dbkv, err = rv.dbfile.OpenKVStoreDefault(rv.kvconfig)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (s *Store) get(key []byte) ([]byte, error) {
	res, err := s.dbkv.GetKV(key)
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
	return s.dbkv.SetKV(key, val)
}

func (s *Store) delete(key []byte) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	return s.deletelocked(key)
}

func (s *Store) deletelocked(key []byte) error {
	return s.dbkv.DeleteKV(key)
}

func (s *Store) commit() error {
	return s.dbfile.Commit(forestdb.COMMIT_NORMAL)
}

func (s *Store) Close() error {
	err := s.dbkv.Close()
	if err != nil {
		return err
	}
	return s.dbfile.Close()

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

func (s *Store) getSeqNum() (forestdb.SeqNum, error) {
	dbinfo, err := s.dbkv.Info()
	if err != nil {
		return 0, err
	}
	return dbinfo.LastSeqNum(), nil
}

func (s *Store) newSnapshot() (*forestdb.KVStore, error) {
	seqNum, err := s.getSeqNum()
	if err != nil {
		return nil, err
	}
	return s.dbkv.SnapshotOpen(seqNum)
}

func (s *Store) getRollbackID() ([]byte, error) {
	seqNum, err := s.getSeqNum()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, seqNum)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Store) rollbackTo(rollbackId []byte) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	buf := bytes.NewReader(rollbackId)
	var seqNum forestdb.SeqNum
	err := binary.Read(buf, binary.LittleEndian, &seqNum)
	if err != nil {
		return err
	}
	err = s.dbkv.Rollback(seqNum)
	if err != nil {
		return err
	}
	return nil
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
