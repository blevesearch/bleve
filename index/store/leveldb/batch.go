//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
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
