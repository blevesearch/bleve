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
	"github.com/blevesearch/bleve/search"
)

type TermQuery struct {
	Term     string  `json:"term"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

func NewTermQuery(term string) *TermQuery {
	return &TermQuery{
		Term:     term,
		BoostVal: 1.0,
	}
}

func (q *TermQuery) Boost() float64 {
	return q.BoostVal
}

func (q *TermQuery) SetBoost(b float64) *TermQuery {
	q.BoostVal = b
	return q
}

func (q *TermQuery) Field() string {
	return q.FieldVal
}

func (q *TermQuery) SetField(f string) *TermQuery {
	q.FieldVal = f
	return q
}

func (q *TermQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.DefaultField
	}
	return search.NewTermSearcher(i.i, q.Term, field, q.BoostVal, explain)
}

func (q *TermQuery) Validate() error {
	return nil
}
