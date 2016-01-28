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
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type regexpQuery struct {
	Regexp   string  `json:"regexp"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
	compiled *regexp.Regexp
}

// NewRegexpQuery creates a new Query which finds
// documents containing terms that match the
// specified regular expression.
func NewRegexpQuery(regexp string) *regexpQuery {
	return &regexpQuery{
		Regexp:   regexp,
		BoostVal: 1.0,
	}
}

func (q *regexpQuery) Boost() float64 {
	return q.BoostVal
}

func (q *regexpQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *regexpQuery) Field() string {
	return q.FieldVal
}

func (q *regexpQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *regexpQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	err := q.compile()
	if err != nil {
		return nil, err
	}

	return searchers.NewRegexpSearcher(i, q.compiled, field, q.BoostVal, explain)
}

func (q *regexpQuery) Validate() error {
	return q.compile()
}

func (q *regexpQuery) compile() error {
	if q.compiled == nil {
		// require that pattern be anchored to start and end of term
		actualRegexp := q.Regexp
		if !strings.HasPrefix(actualRegexp, "^") {
			actualRegexp = "^" + actualRegexp
		}
		if !strings.HasSuffix(actualRegexp, "$") {
			actualRegexp = actualRegexp + "$"
		}
		var err error
		q.compiled, err = regexp.Compile(actualRegexp)
		if err != nil {
			return err
		}
	}
	return nil
}
