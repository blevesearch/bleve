//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

// All of the bolt impls provide a layer of indirection to allow for processing
// of values as they are read/written to bolt depending on the key or bucket name
// This is used to allow better support for file callbacks

// wrapper around bolt.DB
type RootBoltImpl struct {
	*bolt.DB
}

// wrapper around bolt.Tx
type BoltTxImpl struct {
	*bolt.Tx
}

// wrapper around bolt.Bucket
type BoltBucketImpl struct {
	*bolt.Bucket

	name string // store the name of the bucket during creation
}

func OpenBolt(path string, mode os.FileMode, options *bolt.Options) (*RootBoltImpl, error) {
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &RootBoltImpl{DB: db}, nil
}

func (r *RootBoltImpl) Begin(writable bool) (*BoltTxImpl, error) {
	tx, err := r.DB.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &BoltTxImpl{Tx: tx}, nil
}

func (r *RootBoltImpl) View(fn func(*BoltTxImpl) error) error {
	return r.DB.View(func(tx *bolt.Tx) error {
		return fn(&BoltTxImpl{Tx: tx})
	})
}

func (r *RootBoltImpl) Update(fn func(*BoltTxImpl) error) error {
	return r.DB.Update(func(tx *bolt.Tx) error {
		return fn(&BoltTxImpl{Tx: tx})
	})
}

func (tx *BoltTxImpl) CreateBucketIfNotExists(name []byte) (*BoltBucketImpl, error) {
	bucket, err := tx.Tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return &BoltBucketImpl{
		name:   string(name),
		Bucket: bucket,
	}, nil
}

func (tx *BoltTxImpl) Bucket(name []byte) *BoltBucketImpl {
	bucket := tx.Tx.Bucket(name)
	if bucket == nil {
		return nil
	}
	return &BoltBucketImpl{
		name:   string(name),
		Bucket: bucket,
	}
}

func (b *BoltBucketImpl) GetBucket(name []byte) *BoltBucketImpl {
	bucket := b.Bucket.Bucket(name)
	if bucket == nil {
		return nil
	}
	return &BoltBucketImpl{
		name:   string(name),
		Bucket: bucket,
	}
}

func (b *BoltBucketImpl) CreateBucketIfNotExists(name []byte) (*BoltBucketImpl, error) {
	bucket, err := b.Bucket.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	return &BoltBucketImpl{
		name:   string(name),
		Bucket: bucket,
	}, nil
}

// Process values during ForEach if the bucket name or key is in the boltKeysProcessed map
func (b *BoltBucketImpl) ForEach(fn func(key []byte, value []byte) error, reader FileReader) error {
	_, ok1 := boltKeysProcessed[b.name]
	return b.Bucket.ForEach(func(k, v []byte) error {
		if _, ok2 := boltKeysProcessed[string(k)]; ok1 || ok2 {
			if reader == nil {
				return fmt.Errorf("reader callback is required for bucket %s", b.name)
			}
			processedValue, err := reader.Process(v)
			if err != nil {
				return err
			}
			return fn(k, processedValue)
		}
		return fn(k, v)
	})
}

// Process values during Put/Get if the bucket name or key is in the boltKeysProcessed map
func (b *BoltBucketImpl) Put(key []byte, value []byte, writer FileWriter) error {
	_, ok1 := boltKeysProcessed[string(key)]
	_, ok2 := boltKeysProcessed[b.name]
	if ok1 || ok2 {
		if writer == nil {
			return fmt.Errorf("writer callback is required for key %s", string(key))
		}
		processedValue := writer.Process(value)
		return b.Bucket.Put(key, processedValue)
	}
	return b.Bucket.Put(key, value)
}

// Process values during Put/Get if the bucket name or key is in the boltKeysProcessed map
func (b *BoltBucketImpl) Get(key []byte, reader FileReader) ([]byte, error) {
	_, ok1 := boltKeysProcessed[string(key)]
	_, ok2 := boltKeysProcessed[b.name]
	if ok1 || ok2 {
		if reader == nil {
			return nil, fmt.Errorf("reader callback is required for key %s", string(key))
		}
		val := b.Bucket.Get(key)
		if val == nil {
			return nil, nil
		}
		processedVal, err := reader.Process(val)
		if err != nil {
			return nil, err
		}
		return processedVal, nil
	}
	return b.Bucket.Get(key), nil
}
