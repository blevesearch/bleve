//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package query

import (
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searcher"
)

type RegexpQuery struct {
	Regexp   string `json:"regexp"`
	Field    string `json:"field,omitempty"`
	Boost    *Boost `json:"boost,omitempty"`
	compiled *regexp.Regexp
}

// NewRegexpQuery creates a new Query which finds
// documents containing terms that match the
// specified regular expression.
func NewRegexpQuery(regexp string) *RegexpQuery {
	return &RegexpQuery{
		Regexp: regexp,
	}
}

func (q *RegexpQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.Boost = &boost
}

func (q *RegexpQuery) SetField(f string) {
	q.Field = f
}

func (q *RegexpQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {
	field := q.Field
	if q.Field == "" {
		field = m.DefaultSearchField()
	}
	err := q.compile()
	if err != nil {
		return nil, err
	}

	return searcher.NewRegexpSearcher(i, q.compiled, field, q.Boost.Value(), explain)
}

func (q *RegexpQuery) Validate() error {
	return q.compile()
}

func (q *RegexpQuery) compile() error {
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
