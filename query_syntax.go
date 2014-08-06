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
	"github.com/couchbaselabs/bleve/search"
)

type SyntaxQuery struct {
	Query           string  `json:"query"`
	DefaultFieldVal string  `json:"default_field,omitempty"`
	BoostVal        float64 `json:"boost,omitempty"`
}

func NewSyntaxQuery(query string) *SyntaxQuery {
	return &SyntaxQuery{
		Query:    query,
		BoostVal: 1.0,
	}
}

func (q *SyntaxQuery) Boost() float64 {
	return q.BoostVal
}

func (q *SyntaxQuery) SetBoost(b float64) *SyntaxQuery {
	q.BoostVal = b
	return q
}

func (q *SyntaxQuery) DefaultField() string {
	return q.DefaultFieldVal
}

func (q *SyntaxQuery) SetField(f string) *SyntaxQuery {
	q.DefaultFieldVal = f
	return q
}

func (q *SyntaxQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	defaultField := q.DefaultFieldVal
	if q.DefaultFieldVal == "" {
		defaultField = i.m.defaultField()
	}
	newQuery, err := ParseQuerySyntax(q.Query, i.m, defaultField)
	if err != nil {
		return nil, err
	}
	return newQuery.Searcher(i, explain)
}

func (q *SyntaxQuery) Validate() error {
	return nil
}
