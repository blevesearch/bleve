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

type TermBooleanQuery struct {
	Must     *TermConjunctionQuery `json:"must,omitempty"`
	MustNot  *TermDisjunctionQuery `json:"must_not,omitempty"`
	Should   *TermDisjunctionQuery `json:"should,omitempty"`
	BoostVal float64               `json:"boost,omitempty"`
	Explain  bool                  `json:"explain,omitempty"`
}

func (q *TermBooleanQuery) Boost() float64 {
	return q.BoostVal
}

func (q *TermBooleanQuery) Searcher(index index.Index) (Searcher, error) {
	return NewTermBooleanSearcher(index, q)
}

func (q *TermBooleanQuery) Validate() error {
	if q.Must == nil && q.Should == nil {
		return fmt.Errorf("Boolean query must contain at least one MUST or SHOULD clause")
	}
	if q.Must != nil && len(q.Must.Terms) == 0 && q.Should != nil && len(q.Should.Terms) == 0 {
		return fmt.Errorf("Boolean query must contain at least one MUST or SHOULD clause")
	}
	return nil
}
