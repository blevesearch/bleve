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
	"fmt"

	"github.com/blevesearch/bleve/search"
)

type matchQuery struct {
	Match    string  `json:"match"`
	FieldVal string  `json:"field,omitempty"`
	Analyzer string  `json:"analyzer,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

// NewMatchQuery creates a Query for matching text.
// An Analyzer is chosed based on the field.
// Input text is analyzed using this analyzer.
// Token terms resulting from this analysis are
// used to perform term searches.  Result documents
// must satisfy at least one of these term searches.
func NewMatchQuery(match string) *matchQuery {
	return &matchQuery{
		Match:    match,
		BoostVal: 1.0,
	}
}

func (q *matchQuery) Boost() float64 {
	return q.BoostVal
}

func (q *matchQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *matchQuery) Field() string {
	return q.FieldVal
}

func (q *matchQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *matchQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {

	analyzerName := ""
	if q.Analyzer != "" {
		analyzerName = q.Analyzer
	} else {
		analyzerName = i.m.analyzerNameForPath(q.FieldVal)
	}
	analyzer := i.m.analyzerNamed(analyzerName)

	if analyzer == nil {
		return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
	}

	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.DefaultField
	}

	tokens := analyzer.Analyze([]byte(q.Match))
	if len(tokens) > 0 {

		tqs := make([]Query, len(tokens))
		for i, token := range tokens {
			tqs[i] = NewTermQuery(string(token.Term)).
				SetField(field).
				SetBoost(q.BoostVal)
		}

		shouldQuery := NewDisjunctionQueryMin(tqs, 1).
			SetBoost(q.BoostVal)

		return shouldQuery.Searcher(i, explain)
	}
	noneQuery := NewMatchNoneQuery()
	return noneQuery.Searcher(i, explain)
}

func (q *matchQuery) Validate() error {
	return nil
}
