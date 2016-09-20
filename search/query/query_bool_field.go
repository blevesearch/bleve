//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package query

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type BoolFieldQuery struct {
	Bool     bool    `json:"bool"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

// NewBoolFieldQuery creates a new Query for boolean fields
func NewBoolFieldQuery(val bool) *BoolFieldQuery {
	return &BoolFieldQuery{
		Bool:     val,
		BoostVal: 1.0,
	}
}

func (q *BoolFieldQuery) Boost() float64 {
	return q.BoostVal
}

func (q *BoolFieldQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *BoolFieldQuery) Field() string {
	return q.FieldVal
}

func (q *BoolFieldQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *BoolFieldQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	term := "F"
	if q.Bool {
		term = "T"
	}
	return searchers.NewTermSearcher(i, term, field, q.BoostVal, explain)
}

func (q *BoolFieldQuery) Validate() error {
	return nil
}
