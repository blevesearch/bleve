//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type prefixQuery struct {
	Prefix   string  `json:"prefix"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

// NewPrefixQuery creates a new Query which finds
// documents containing terms that start with the
// specified prefix.
func NewPrefixQuery(prefix string) *prefixQuery {
	return &prefixQuery{
		Prefix:   prefix,
		BoostVal: 1.0,
	}
}

func (q *prefixQuery) Boost() float64 {
	return q.BoostVal
}

func (q *prefixQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *prefixQuery) Field() string {
	return q.FieldVal
}

func (q *prefixQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *prefixQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	return searchers.NewTermPrefixSearcher(i, q.Prefix, field, q.BoostVal, explain)
}

func (q *prefixQuery) Validate() error {
	return nil
}
