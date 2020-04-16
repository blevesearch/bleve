//  Copyright (c) 2020 Couchbase, Inc.
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
	"fmt"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
)

type MultiMatchQuery struct {
	Match     string             `json:"match"`
	FieldVals []string           `json:"fields,omitempty"`
	Analyzer  string             `json:"analyzer,omitempty"`
	BoostVal  *Boost             `json:"boost,omitempty"`
	Prefix    int                `json:"prefix_length"`
	Fuzziness int                `json:"fuzziness"`
	Type      string             `json:"type,omitempty"`
	Operator  MatchQueryOperator `json:"operator,omitempty"`
	Min       float64            `json:"min"`
}

const (
	// Documents which match any field (at least one);
	// combines the _score from each field (DEFAULT)
	MatchMostFields = "most_fields"
	// Documents which match all specified field
	MatchAllFields = "all_fields"
)

// NewMultiMatchQuery creates a Query for matching text across multiple fields.
// Analyzers are chosen based on the fields.
// Input text is analyzed using provided analyzer or chosen analyzers.
// Token terms resulting from this analysis are used to perform
// term searches. Result documents must satisfy at least specified number of
// term searches (defaults to 1, configurable via min).
// Result documents must also satisfy any fiel
func NewMultiMatchQuery(match string) *MultiMatchQuery {
	return &MultiMatchQuery{
		Match:    match,
		Operator: MatchQueryOperatorOr,
		Min:      1,
	}
}

func (q *MultiMatchQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *MultiMatchQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *MultiMatchQuery) AddFields(f ...string) {
	q.FieldVals = append(q.FieldVals, f...)
}

func (q *MultiMatchQuery) Fields() []string {
	return q.FieldVals
}

func (q *MultiMatchQuery) SetType(to string) {
	q.Type = to
}

func (q *MultiMatchQuery) SetFuzziness(f int) {
	q.Fuzziness = f
}

func (q *MultiMatchQuery) SetPrefix(p int) {
	q.Prefix = p
}

func (q *MultiMatchQuery) SetOperator(operator MatchQueryOperator) {
	q.Operator = operator
}

func (q *MultiMatchQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	fields := q.FieldVals
	if q.FieldVals == nil {
		fields = []string{m.DefaultSearchField()}
	}

	var tokens analysis.TokenStream
	if len(q.Analyzer) > 0 {
		analyzer := m.AnalyzerNamed(q.Analyzer)
		if analyzer == nil {
			return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
		}

		tokens = analyzer.Analyze([]byte(q.Match))
	}

	fieldQueries := []Query{}
	for _, field := range fields {
		field, boost := extractBoostFromField(field)
		tqs := []Query{}

		if len(q.Analyzer) == 0 {
			analyzerName := m.AnalyzerNameForPath(field)
			analyzer := m.AnalyzerNamed(analyzerName)
			tokens = analyzer.Analyze([]byte(q.Match))
		}

		if len(tokens) > 0 {
			if q.Fuzziness != 0 {
				for _, token := range tokens {
					query := NewFuzzyQuery(string(token.Term))
					query.SetFuzziness(q.Fuzziness)
					query.SetPrefix(q.Prefix)
					query.SetField(field)
					query.SetBoost(boost)
					tqs = append(tqs, query)
				}
			} else {
				for _, token := range tokens {
					tq := NewTermQuery(string(token.Term))
					tq.SetField(field)
					tq.SetBoost(boost)
					tqs = append(tqs, tq)
				}
			}
		}

		// now apply the match query operator on the term queries
		// over this field.
		if len(tqs) > 0 {
			switch q.Operator {
			case MatchQueryOperatorOr:
				shouldQuery := NewDisjunctionQuery(tqs)
				shouldQuery.SetMin(q.Min)
				fieldQueries = append(fieldQueries, shouldQuery)

			case MatchQueryOperatorAnd:
				mustQuery := NewConjunctionQuery(tqs)
				fieldQueries = append(fieldQueries, mustQuery)

			default:
				return nil, fmt.Errorf("unhandled operator %d", q.Operator)
			}
		}
	}

	matchFieldsType := q.Type
	if len(matchFieldsType) == 0 {
		matchFieldsType = MatchMostFields
	}

	switch matchFieldsType {
	case MatchMostFields:
		shouldQuery := NewDisjunctionQuery(fieldQueries)
		shouldQuery.SetBoost(q.BoostVal.Value())
		return shouldQuery.Searcher(i, m, options)

	case MatchAllFields:
		mustQuery := NewConjunctionQuery(fieldQueries)
		mustQuery.SetBoost(q.BoostVal.Value())
		return mustQuery.Searcher(i, m, options)

	default:
		return nil, fmt.Errorf("unhandled fields type %s", q.Type)
	}

	noneQuery := NewMatchNoneQuery()
	return noneQuery.Searcher(i, m, options)
}

// This API looks for a boost setting within a field string.
// For example,
//     - input: "field^2.0" returns "field", "2.0"
//     - input: "field" returns "field, "0"
func extractBoostFromField(val string) (string, float64) {
	arr := strings.Split(val, "^")
	if len(arr) != 2 {
		return val, 0
	}

	if boost, err := strconv.ParseFloat(arr[1], 64); err == nil {
		return arr[0], boost
	}

	return val, 0
}
