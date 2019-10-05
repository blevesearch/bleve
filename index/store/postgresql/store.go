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
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	// needed because we initialize the database connection instance in this file
	_ "github.com/lib/pq"
)

const (
	// Name is the name for this store.
	Name = "postgresql"
)

func init() {
	registry.RegisterKVStore(Name, New)
}

// Store is a PostgreSQL implementation of the bleve Key/Value store.
type Store struct {
	db *sql.DB

	table  string
	keyCol string
	valCol string

	mo store.MergeOperator
}

// New creates a new instance of a PostgreSQL Store.
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	datasourceName, ok := config["datasourceName"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify datasourceName")
	}
	if datasourceName == "" {
		return nil, os.ErrInvalid
	}

	table, ok := config["table"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify table")
	}
	if table == "" {
		return nil, os.ErrInvalid
	}

	keyCol, ok := config["keyCol"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify keyCol")
	}
	if keyCol == "" {
		return nil, os.ErrInvalid
	}

	valCol, ok := config["valCol"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify valCol")
	}
	if valCol == "" {
		return nil, os.ErrInvalid
	}

	db, err := sql.Open("postgres", datasourceName)

	createTableQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s BYTEA PRIMARY KEY, %s BYTEA);",
		table,
		keyCol,
		valCol,
	)
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("could not create table %s: %v", table, err)
	}

	rv := Store{
		mo:     mo,
		db:     db,
		table:  table,
		keyCol: keyCol,
		valCol: valCol,
	}
	return &rv, nil
}

// Writer returns a KVWriter which can be used to
// make changes to the KVStore.  If a writer cannot
// be obtained a non-nil error is returned.
func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		db:     s.db,
		table:  s.table,
		keyCol: s.keyCol,
		valCol: s.valCol,
		mo:     s.mo,
	}, nil
}

// Reader returns a KVReader which can be used to
// read data from the KVStore.  If a reader cannot
// be obtained a non-nil error is returned.
func (s *Store) Reader() (store.KVReader, error) {
	ctx := context.Background()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		log.Printf("could not begin database transaction: %v", err)
		return nil, err
	}

	return &Reader{
		tx:     tx,
		table:  s.table,
		keyCol: s.keyCol,
		valCol: s.valCol,
	}, nil
}

// Close closes the KVStore
func (s *Store) Close() error {
	return s.db.Close()
}
