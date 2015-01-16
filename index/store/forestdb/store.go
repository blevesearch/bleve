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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/couchbaselabs/goforestdb"
)

type ForestDBConfig struct {
	BlockSize                 uint32
	BufferCacheSize           uint64
	ChunkSize                 uint16
	CleanupCacheOnClose       bool
	CompactionBufferSizeMax   uint32
	CompactionMinimumFilesize uint64
	CompactionMode            forestdb.CompactOpt
	CompactionThreshold       uint8
	CompactorSleepDuration    uint64
	CompressDocumentBody      bool
	DurabilityOpt             forestdb.DurabilityOpt
	OpenFlags                 forestdb.OpenFlags
	PurgingInterval           uint32
	SeqTreeOpt                forestdb.SeqTreeOpt
	WalFlushBeforeCommit      bool
	WalThreshold              uint64
}

const Name = "forestdb"

type Store struct {
	path     string
	config   *forestdb.Config
	kvconfig *forestdb.KVStoreConfig
	dbfile   *forestdb.File
	dbkv     *forestdb.KVStore
	writer   sync.Mutex
}

func Open(path string, createIfMissing bool,
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
	return Open(path, createIfMissing, config)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func applyConfig(c *forestdb.Config, config map[string]interface{}) (
	*forestdb.Config, error) {
	v, exists := config["forestDBConfig"]
	if !exists || v == nil {
		return c, nil
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return c, nil
	}
	// These extra steps of json.Marshal()/Unmarshal() help to convert
	// to the types that we need for the setter calls.
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var f ForestDBConfig
	err = json.Unmarshal(b, &f)
	if err != nil {
		return nil, err
	}
	if _, exists := m["blockSize"]; exists {
		c.SetBlockSize(f.BlockSize)
	}
	if _, exists := m["bufferCacheSize"]; exists {
		c.SetBufferCacheSize(f.BufferCacheSize)
	}
	if _, exists := m["chunkSize"]; exists {
		c.SetChunkSize(f.ChunkSize)
	}
	if _, exists := m["cleanupCacheOnClose"]; exists {
		c.SetCleanupCacheOnClose(f.CleanupCacheOnClose)
	}
	if _, exists := m["compactionBufferSizeMax"]; exists {
		c.SetCompactionBufferSizeMax(f.CompactionBufferSizeMax)
	}
	if _, exists := m["compactionMinimumFilesize"]; exists {
		c.SetCompactionMinimumFilesize(f.CompactionMinimumFilesize)
	}
	if _, exists := m["compactionMode"]; exists {
		c.SetCompactionMode(f.CompactionMode)
	}
	if _, exists := m["compactionThreshold"]; exists {
		c.SetCompactionThreshold(f.CompactionThreshold)
	}
	if _, exists := m["compactorSleepDuration"]; exists {
		c.SetCompactorSleepDuration(f.CompactorSleepDuration)
	}
	if _, exists := m["compressDocumentBody"]; exists {
		c.SetCompressDocumentBody(f.CompressDocumentBody)
	}
	if _, exists := m["durabilityOpt"]; exists {
		c.SetDurabilityOpt(f.DurabilityOpt)
	}
	if _, exists := m["openFlags"]; exists {
		c.SetOpenFlags(f.OpenFlags)
	}
	if _, exists := m["purgingInterval"]; exists {
		c.SetPurgingInterval(f.PurgingInterval)
	}
	if _, exists := m["seqTreeOpt"]; exists {
		c.SetSeqTreeOpt(f.SeqTreeOpt)
	}
	if _, exists := m["walFlushBeforeCommit"]; exists {
		c.SetWalFlushBeforeCommit(f.WalFlushBeforeCommit)
	}
	if _, exists := m["walThreshold"]; exists {
		c.SetWalThreshold(f.WalThreshold)
	}
	return c, nil
}
