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
	_, b.err = b.tx.Exec(insertQuery, key, val)
	if b.err != nil {
		log.Printf("could not add Set op to batch: %v", b.err)
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

	_, b.err = b.tx.Exec(deleteQuery, key)
	if b.err != nil {
		log.Printf("could not add Delete op to batch: %v", b.err)
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
