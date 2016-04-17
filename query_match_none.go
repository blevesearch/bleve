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
	"encoding/json"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type matchNoneQuery struct {
	BoostVal float64 `json:"boost,omitempty"`
}

// NewMatchNoneQuery creates a Query which will not
// match any documents in the index.
func NewMatchNoneQuery() *matchNoneQuery {
	return &matchNoneQuery{
		BoostVal: 1.0,
	}
}

func (q *matchNoneQuery) Boost() float64 {
	return q.BoostVal
}

func (q *matchNoneQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *matchNoneQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	return searchers.NewMatchNoneSearcher(i)
}

func (q *matchNoneQuery) Validate() error {
	return nil
}

func (q *matchNoneQuery) Field() string {
	return ""
}

func (q *matchNoneQuery) SetField(f string) Query {
	return q
}

func (q *matchNoneQuery) MarshalJSON() ([]byte, error) {
	tmp := map[string]interface{}{
		"boost":      q.BoostVal,
		"match_none": map[string]interface{}{},
	}
	return json.Marshal(tmp)
}
