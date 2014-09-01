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
	"fmt"

	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type booleanQuery struct {
	Must     Query   `json:"must,omitempty"`
	Should   Query   `json:"should,omitempty"`
	MustNot  Query   `json:"must_not,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

// NewBooleanQuery creates a compound Query composed
// of several other Query objects.
// Result documents must satisify ALL of the
// must Queries.
// Result documents must satsify NONE of the must not
// Queries.
// If there are any should queries, result documents
// must satisfy at least one of them.
func NewBooleanQuery(must []Query, should []Query, mustNot []Query) *booleanQuery {
	min := 0.0
	if len(should) > 0 {
		min = 1.0
	}
	return NewBooleanQueryMinShould(must, should, mustNot, min)
}

// NewBooleanQueryMinShould is the same as
// NewBooleanQuery, only it offers control of the
// minimum number of should queries that must be
// satisfied.
func NewBooleanQueryMinShould(must []Query, should []Query, mustNot []Query, minShould float64) *booleanQuery {

	rv := booleanQuery{
		BoostVal: 1.0,
	}
	if len(must) > 0 {
		rv.Must = NewConjunctionQuery(must)
	}
	if len(should) > 0 {
		rv.Should = NewDisjunctionQueryMin(should, minShould)
	}
	if len(mustNot) > 0 {
		rv.MustNot = NewDisjunctionQuery(mustNot)
	}

	return &rv
}

func (q *booleanQuery) Boost() float64 {
	return q.BoostVal
}

func (q *booleanQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *booleanQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	var err error

	var mustSearcher search.Searcher
	if q.Must != nil {
		mustSearcher, err = q.Must.Searcher(i, explain)
		if err != nil {
			return nil, err
		}
	}

	var shouldSearcher search.Searcher
	if q.Should != nil {
		shouldSearcher, err = q.Should.Searcher(i, explain)
		if err != nil {
			return nil, err
		}
	}

	var mustNotSearcher search.Searcher
	if q.MustNot != nil {
		mustNotSearcher, err = q.MustNot.Searcher(i, explain)
		if err != nil {
			return nil, err
		}
	}

	return searchers.NewBooleanSearcher(i.i, mustSearcher, shouldSearcher, mustNotSearcher, explain)
}

func (q *booleanQuery) Validate() error {
	if q.Must != nil {
		err := q.Must.Validate()
		if err != nil {
			return err
		}
	}
	if q.Should != nil {
		err := q.Should.Validate()
		if err != nil {
			return err
		}
	}
	if q.MustNot != nil {
		err := q.MustNot.Validate()
		if err != nil {
			return err
		}
	}
	if q.Must == nil && q.Should == nil {
		return ERROR_BOOLEAN_QUERY_NEEDS_MUST_OR_SHOULD
	}
	return nil
}

func (q *booleanQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Must     json.RawMessage `json:"must,omitempty"`
		Should   json.RawMessage `json:"should,omitempty"`
		MustNot  json.RawMessage `json:"must_not,omitempty"`
		BoostVal float64         `json:"boost,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	if tmp.Must != nil {
		q.Must, err = ParseQuery(tmp.Must)
		if err != nil {
			return err
		}
		_, isConjunctionQuery := q.Must.(*conjunctionQuery)
		if !isConjunctionQuery {
			return fmt.Errorf("must clause must be conjunction")
		}
	}

	if tmp.Should != nil {
		q.Should, err = ParseQuery(tmp.Should)
		if err != nil {
			return err
		}
		_, isDisjunctionQuery := q.Should.(*disjunctionQuery)
		if !isDisjunctionQuery {
			return fmt.Errorf("should clause must be disjunction")
		}
	}

	if tmp.MustNot != nil {
		q.MustNot, err = ParseQuery(tmp.MustNot)
		if err != nil {
			return err
		}
		_, isDisjunctionQuery := q.MustNot.(*disjunctionQuery)
		if !isDisjunctionQuery {
			return fmt.Errorf("must not clause must be disjunction")
		}
	}

	q.BoostVal = tmp.BoostVal
	if q.BoostVal == 0 {
		q.BoostVal = 1
	}
	return nil
}

func (q *booleanQuery) Field() string {
	return ""
}

func (q *booleanQuery) SetField(f string) Query {
	return q
}
