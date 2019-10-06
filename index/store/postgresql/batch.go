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

// Batch is an abstraction for making multiple KV mutations at once
type Batch struct {
	db *sql.DB
	tx *sql.Tx

	table  string
	keyCol string
	valCol string

	mo    store.MergeOperator
	merge *store.EmulatedMerge

	err error
}

// Set updates the key with the specified value
// both key and value []byte may be reused as soon as this call returns
func (b *Batch) Set(key, val []byte) {
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s, %s) VALUES ($1, $2) ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s;",
		b.table,
		b.keyCol,
		b.valCol,
		b.keyCol,
		b.valCol,
		b.valCol,
	)

	var stmt *sql.Stmt
	stmt, b.err = b.tx.Prepare(insertQuery)
	if b.err != nil {
		log.Printf("could not prepare statement for Set operation in batch: %v", b.err)
	}

	_, b.err = stmt.Exec(key, val)
	if b.err != nil {
		log.Printf("could not add Set operation to batch: %v", b.err)
	}
}

// Delete removes the specified key
// the key []byte may be reused as soon as this call returns
func (b *Batch) Delete(key []byte) {
	deleteQuery := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = $1;",
		b.table,
		b.keyCol,
	)

	var stmt *sql.Stmt
	stmt, b.err = b.tx.Prepare(deleteQuery)
	if b.err != nil {
		log.Printf("could not prepare statement for Delete operation in batch: %v", b.err)
	}

	_, b.err = stmt.Exec(key)
	if b.err != nil {
		log.Printf("could not add Delete operation to batch: %v", b.err)
	}
}

// Merge merges old value with the new value at the specified key
// as prescribed by the KVStores merge operator
// both key and value []byte may be reused as soon as this call returns
func (b *Batch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

// Reset frees resources for this batch and allows reuse
func (b *Batch) Reset() {
	if b.tx != nil {
		b.err = b.tx.Rollback()
		if b.err != nil {
			log.Printf("could not roll back transaction for Reset: %v", b.err)
		}
	}

	b.tx, b.err = b.db.Begin()
	if b.err != nil {
		log.Printf("could not Reset batch: %v", b.err)
	}

	b.merge = store.NewEmulatedMerge(b.mo)
}

// Commit commits the batch
func (b *Batch) Commit() error {
	if b.err != nil {
		// We want to return the initial error here
		err := b.err
		b.Reset()
		return err
	}

	b.err = b.tx.Commit()
	if b.err == nil {
		b.tx = nil
	}

	return b.err
}

// Close frees resources
func (b *Batch) Close() error {
	if b.tx != nil {
		b.err = b.tx.Rollback()
		if b.err != nil {
			log.Printf("could not roll back transaction for Close: %v", b.err)
		}
	}

	b.tx = nil
	b.merge = nil

	return b.err
}
