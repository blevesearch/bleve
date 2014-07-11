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
	"encoding/json"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
)

type TermConjunctionQuery struct {
	Terms    []Query `json:"terms"`
	BoostVal float64 `json:"boost"`
	Explain  bool    `json:"explain"`
	mapping  document.Mapping
}

func (q *TermConjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *TermConjunctionQuery) Searcher(index index.Index) (Searcher, error) {
	return NewTermConjunctionSearcher(index, q)
}

func (q *TermConjunctionQuery) Validate() error {
	return nil
}

func (q *TermConjunctionQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Terms    []json.RawMessage `json:"terms"`
		BoostVal float64           `json:"boost"`
		Explain  bool              `json:"explain"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.Terms = make([]Query, len(tmp.Terms))
	for i, term := range tmp.Terms {
		query, err := ParseQuery(term, q.mapping)
		if err != nil {
			return err
		}
		q.Terms[i] = query
	}
	q.BoostVal = tmp.BoostVal
	q.Explain = tmp.Explain
	return nil
}
