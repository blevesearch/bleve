//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package boltdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	store *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) error {

	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	tx, err := w.store.db.Begin(true)
	if err != nil {
		return err
	}

	bucket := tx.Bucket([]byte(w.store.bucket))

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)
		existingVal := bucket.Get(kb)
		mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		err = bucket.Put(kb, mergedVal)
		if err != nil {
			return err
		}
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			err := bucket.Put(op.K, op.V)
			if err != nil {
				return err
			}
		} else {
			err := bucket.Delete(op.K)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (w *Writer) Close() error {
	return nil
}
