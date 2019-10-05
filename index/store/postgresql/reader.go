package postgresql

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/blevesearch/bleve/index/store"
)

// Reader is an abstraction of an **ISOLATED** reader
// In this context isolated is defined to mean that
// writes/deletes made after the Reader is opened
// are not observed.
// Because there is usually a cost associated with
// keeping isolated readers active, users should
// close them as soon as they are no longer needed.
type Reader struct {
	tx *sql.Tx

	table  string
	keyCol string
	valCol string
}

// Get returns the value associated with the key
// If the key does not exist, nil is returned.
// The caller owns the bytes returned.
func (r *Reader) Get(key []byte) ([]byte, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = $1;",
		r.valCol,
		r.table,
		r.keyCol,
	)

	var bts []byte

	err := r.tx.QueryRow(query, key).Scan(&bts)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil && err != sql.ErrNoRows {
		log.Printf("could not query row for Get: %v", err)
		return nil, err
	}

	rv := make([]byte, len(bts))
	copy(rv, bts)

	return rv, nil
}

// MultiGet retrieves multiple values in one call.
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = ANY $1;",
		r.valCol,
		r.table,
		r.keyCol,
	)

	rows, err := r.tx.Query(query, keys)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Printf("could not query for MultiGet: %v", err)
		return nil, err
	}
	defer rows.Close()

	vals := make([][]byte, 0)
	for rows.Next() {
		var bts []byte

		err := rows.Scan(&bts)
		if err != nil {
			log.Printf("could not scan row for MultiGet: %v", err)
			return nil, err
		}

		val := make([]byte, len(bts))
		copy(val, bts)

		vals = append(vals, val)
	}

	return vals, nil
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := &Iterator{
		tx:     r.tx,
		table:  r.table,
		keyCol: r.keyCol,
		valCol: r.valCol,
		prefix: prefix,
	}

	rv.Seek(prefix)

	return rv
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := &Iterator{
		tx:     r.tx,
		table:  r.table,
		keyCol: r.keyCol,
		valCol: r.valCol,
		start:  start,
		end:    end,
	}

	rv.Seek(start)

	return rv
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.tx.Rollback()
}
