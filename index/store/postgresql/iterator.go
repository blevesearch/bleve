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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
)

// Iterator is an abstraction around key iteration
type Iterator struct {
	tx *sql.Tx

	table  string
	keyCol string
	valCol string

	prefix []byte
	start  []byte
	end    []byte

	key []byte
	val []byte

	err error
}

func (i *Iterator) seekQueryRow(ctx context.Context, key []byte) *sql.Row {
	if i.prefix != nil && i.end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 AND %s LIKE $2 AND %s < $3 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		prefix := i.prefix
		prefix = append(prefix, '%')

		return stmt.QueryRow(key, prefix, i.end)
	}

	if i.prefix != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 AND %s LIKE $2 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		prefix := i.prefix
		prefix = append(prefix, '%')

		return stmt.QueryRow(key, prefix)
	}

	if i.end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		return stmt.QueryRow(key, i.end)
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s >= $1 ORDER BY %s LIMIT 1;",
		i.keyCol,
		i.valCol,
		i.table,
		i.keyCol,
		i.keyCol,
	)

	stmt, err := i.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for seek: %v", err)
		return nil
	}

	return stmt.QueryRow(key)
}

// Seek will advance the iterator to the specified key
func (i *Iterator) Seek(key []byte) {
	ctx := context.Background()

	if key == nil {
		key = []byte{0}
	}
	if i.start != nil && bytes.Compare(key, i.start) < 0 {
		key = i.start
	}

	i.err = i.seekQueryRow(ctx, key).Scan(&i.key, &i.val)
	if i.err != nil && i.err != sql.ErrNoRows {
		log.Printf("could not query row for Seek: %v", i.err)
	}
}

func (i *Iterator) nextQueryRow(ctx context.Context) *sql.Row {
	if i.prefix != nil && i.end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s > $1 AND %s LIKE $2 AND %s < $3 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		prefix := i.prefix
		prefix = append(prefix, '%')

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		return stmt.QueryRow(i.key, prefix, i.end)
	}

	if i.prefix != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s > $1 AND %s LIKE $2 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		prefix := i.prefix
		prefix = append(prefix, '%')

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		return stmt.QueryRow(i.key, prefix)
	}

	if i.end != nil {
		query := fmt.Sprintf(
			"SELECT %s, %s FROM %s WHERE %s > $1 AND %s < $2 ORDER BY %s LIMIT 1;",
			i.keyCol,
			i.valCol,
			i.table,
			i.keyCol,
			i.keyCol,
			i.keyCol,
		)

		stmt, err := i.tx.Prepare(query)
		if err != nil {
			log.Printf("could not prepare statement for seek: %v", err)
			return nil
		}

		return stmt.QueryRow(i.key, i.end)
	}

	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s > $1 ORDER BY %s LIMIT 1;",
		i.keyCol,
		i.valCol,
		i.table,
		i.keyCol,
		i.keyCol,
	)

	stmt, err := i.tx.Prepare(query)
	if err != nil {
		log.Printf("could not prepare statement for seek: %v", err)
		return nil
	}

	return stmt.QueryRow(i.key)
}

// Next will advance the iterator to the next key
func (i *Iterator) Next() {
	ctx := context.Background()

	i.err = i.nextQueryRow(ctx).Scan(&i.key, &i.val)
	if i.err != nil && i.err != sql.ErrNoRows {
		log.Printf("could not query row for Next: %v", i.err)
	}
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Key() []byte {
	if i.err != nil {
		return nil
	}
	return i.key
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i *Iterator) Value() []byte {
	if i.err != nil {
		return nil
	}
	return i.val
}

// Valid returns whether or not the iterator is in a valid state
func (i *Iterator) Valid() bool {
	return i.err == nil
}

// Current returns Key(),Value(),Valid() in a single operation
func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.Key(), i.Value(), i.Valid()
}

// Close closes the iterator
func (i *Iterator) Close() error {
	// ctx := context.Background()

	// return i.tx.Rollback(ctx)
	return nil
}
