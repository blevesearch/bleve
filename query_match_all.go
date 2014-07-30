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

type MatchAllQuery struct {
	BoostVal float64 `json:"boost,omitempty"`
}

func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{
		BoostVal: 1.0,
	}
}

func (q *MatchAllQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchAllQuery) SetBoost(b float64) *MatchAllQuery {
	q.BoostVal = b
	return q
}

func (q *MatchAllQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	return search.NewMatchAllSearcher(i.i, q.BoostVal, explain)
}

func (q *MatchAllQuery) Validate() error {
	return nil
}
