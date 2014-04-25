//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"fmt"

	"github.com/couchbaselabs/bleve/index"
)

type TermDisjunctionQuery struct {
	Terms    []Query `json:"terms"`
	BoostVal float64 `json:"boost"`
	Explain  bool    `json:"explain"`
	Min      float64 `json:"min"`
}

func (q *TermDisjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *TermDisjunctionQuery) Searcher(index index.Index) (Searcher, error) {
	return NewTermDisjunctionSearcher(index, q)
}

func (q *TermDisjunctionQuery) Validate() error {
	if int(q.Min) > len(q.Terms) {
		return fmt.Errorf("Minimum clauses in disjunction exceeds total number of clauses")
	}
	return nil
}
