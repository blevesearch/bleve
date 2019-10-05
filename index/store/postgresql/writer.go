//  Copyright (c) 2015 Couchbase, Inc.
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

package postgresql

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/blevesearch/bleve/index/store"
)

// Writer is an abstraction for mutating the KVStore
// Writer does **NOT** enforce restrictions of a single writer
// if the underlying KVStore allows concurrent writes, the
// Writer interface should also do so, it is up to the caller
// to do this in a way that is safe and makes sense
type Writer struct {
	db *sql.DB

	table  string
	keyCol string
	valCol string

	mo store.MergeOperator
}

// NewBatch returns a KVBatch for performing batch operations on this kvstore
func (w *Writer) NewBatch() store.KVBatch {
	tx, err := w.db.Begin()
	if err != nil {
		log.Printf("could not start transaction for ExecuteBatch: %v", err)
		return nil
	}

	return &Batch{
		db:     w.db,
		tx:     tx,
		table:  w.table,
		keyCol: w.keyCol,
		valCol: w.valCol,
		mo:     w.mo,
		merge:  store.NewEmulatedMerge(w.mo),
	}
}

// NewBatchEx returns a KVBatch and an associated byte array
// that's pre-sized based on the KVBatchOptions.  The caller can
// use the returned byte array for keys and values associated with
// the batch.  Once the batch is either executed or closed, the
// associated byte array should no longer be accessed by the
// caller.
func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

// ExecuteBatch will execute the KVBatch, the provided KVBatch **MUST** have
// been created by the same KVStore (though not necessarily the same Writer)
// Batch execution is atomic, either all the operations or none will be performed
func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		kb := []byte(k)

		existingValQuery := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s = $1;",
			w.valCol,
			w.table,
			w.keyCol,
		)

		var existingVal []byte
		err := w.db.QueryRow(existingValQuery, kb).Scan(&existingVal)
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		mergedVal, fullMergeOk := w.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}

		// add the final merge to this batch
		batch.Set(kb, mergedVal)
	}

	return batch.Commit()
}

// Close closes the writer
func (w *Writer) Close() error {
	return nil
}
