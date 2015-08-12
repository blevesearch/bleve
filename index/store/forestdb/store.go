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
	"github.com/couchbase/goforestdb"
)

const Name = "forestdb"

type Store struct {
	path     string
	config   *forestdb.Config
	kvconfig *forestdb.KVStoreConfig
	dbfile   *forestdb.File
	dbkv     *forestdb.KVStore
	writer   sync.Mutex
	mo       store.MergeOperator
}

func New(path string, createIfMissing bool,
	config map[string]interface{}) (*Store, error) {
	if config == nil {
		config = map[string]interface{}{}
	}

	forestDBDefaultConfig := forestdb.DefaultConfig()
	forestDBDefaultConfig.SetCompactionMode(forestdb.COMPACT_AUTO)
	forestDBConfig, err := applyConfig(forestDBDefaultConfig, config)
	if err != nil {
		return nil, err
	}

	rv := Store{
		path:     path,
		config:   forestDBConfig,
		kvconfig: forestdb.DefaultKVStoreConfig(),
	}

	if createIfMissing {
		rv.kvconfig.SetCreateIfMissing(true)
	}

	return &rv, nil
}

func (s *Store) Open() error {
	var err error
	s.dbfile, err = forestdb.Open(s.path, s.config)
	if err != nil {
		return err
	}

	s.dbkv, err = s.dbfile.OpenKVStoreDefault(s.kvconfig)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) SetMergeOperator(mo store.MergeOperator) {
	s.mo = mo
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
		return nil, fmt.Errorf("error getting snapshot seqnum: %v", err)
	}
	snapshot, err := s.dbkv.SnapshotOpen(seqNum)
	if err == forestdb.RESULT_NO_DB_INSTANCE {
		checkAgainSeqNum, err := s.getSeqNum()
		if err != nil {
			return nil, fmt.Errorf("error getting snapshot seqnum again: %v", err)
		}
		return nil, fmt.Errorf("cannot open snapshot %v, checked again its %v, error: %v", seqNum, checkAgainSeqNum, err)
	}
	return snapshot, err
}

func (s *Store) GetRollbackID() ([]byte, error) {
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

func (s *Store) RollbackTo(rollbackId []byte) error {
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
	return New(path, createIfMissing, config)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func applyConfig(c *forestdb.Config, config map[string]interface{}) (
	*forestdb.Config, error) {

	if v, exists := config["blockSize"].(float64); exists {
		c.SetBlockSize(uint32(v))
	}
	if v, exists := config["bufferCacheSize"].(float64); exists {
		c.SetBufferCacheSize(uint64(v))
	}
	if v, exists := config["chunkSize"].(float64); exists {
		c.SetChunkSize(uint16(v))
	}
	if v, exists := config["cleanupCacheOnClose"].(bool); exists {
		c.SetCleanupCacheOnClose(v)
	}
	if v, exists := config["compactionBufferSizeMax"].(float64); exists {
		c.SetCompactionBufferSizeMax(uint32(v))
	}
	if v, exists := config["compactionMinimumFilesize"].(float64); exists {
		c.SetCompactionMinimumFilesize(uint64(v))
	}
	if v, exists := config["compactionMode"].(string); exists {
		switch v {
		case "manual":
			c.SetCompactionMode(forestdb.COMPACT_MANUAL)
		case "auto":
			c.SetCompactionMode(forestdb.COMPACT_AUTO)
		default:
			return nil, fmt.Errorf("Unknown compaction mode: %s", v)
		}

	}
	if v, exists := config["compactionThreshold"].(float64); exists {
		c.SetCompactionThreshold(uint8(v))
	}
	if v, exists := config["compactorSleepDuration"].(float64); exists {
		c.SetCompactorSleepDuration(uint64(v))
	}
	if v, exists := config["compressDocumentBody"].(bool); exists {
		c.SetCompressDocumentBody(v)
	}
	if v, exists := config["durabilityOpt"].(string); exists {
		switch v {
		case "none":
			c.SetDurabilityOpt(forestdb.DRB_NONE)
		case "odirect":
			c.SetDurabilityOpt(forestdb.DRB_ODIRECT)
		case "async":
			c.SetDurabilityOpt(forestdb.DRB_ASYNC)
		case "async_odirect":
			c.SetDurabilityOpt(forestdb.DRB_ODIRECT_ASYNC)
		default:
			return nil, fmt.Errorf("Unknown durability option: %s", v)
		}

	}
	if v, exists := config["openFlags"].(string); exists {
		switch v {
		case "create":
			c.SetOpenFlags(forestdb.OPEN_FLAG_CREATE)
		case "readonly":
			c.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)
		default:
			return nil, fmt.Errorf("Unknown open flag: %s", v)
		}
	}
	if v, exists := config["purgingInterval"].(float64); exists {
		c.SetPurgingInterval(uint32(v))
	}
	if v, exists := config["seqTreeOpt"].(bool); exists {
		if !v {
			c.SetSeqTreeOpt(forestdb.SEQTREE_NOT_USE)
		}
	}
	if v, exists := config["walFlushBeforeCommit"].(bool); exists {
		c.SetWalFlushBeforeCommit(v)
	}
	if v, exists := config["walThreshold"].(float64); exists {
		c.SetWalThreshold(uint64(v))
	}
	return c, nil
}
