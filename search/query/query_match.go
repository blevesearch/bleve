//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package query

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
)

type MatchQuery struct {
	Match        string             `json:"match"`
	FieldVal     string             `json:"field,omitempty"`
	Analyzer     string             `json:"analyzer,omitempty"`
	BoostVal     float64            `json:"boost,omitempty"`
	PrefixVal    int                `json:"prefix_length"`
	FuzzinessVal int                `json:"fuzziness"`
	OperatorVal  MatchQueryOperator `json:"operator,omitempty"`
}

type MatchQueryOperator int

const (
	// Document must satisfy AT LEAST ONE of term searches.
	MatchQueryOperatorOr = 0
	// Document must satisfy ALL of term searches.
	MatchQueryOperatorAnd = 1
)

func (o MatchQueryOperator) MarshalJSON() ([]byte, error) {
	switch o {
	case MatchQueryOperatorOr:
		return json.Marshal("or")
	case MatchQueryOperatorAnd:
		return json.Marshal("and")
	default:
		return nil, fmt.Errorf("cannot marshal match operator %d to JSON", o)
	}
}

func (o *MatchQueryOperator) UnmarshalJSON(data []byte) error {
	var operatorString string
	err := json.Unmarshal(data, &operatorString)
	if err != nil {
		return err
	}

	switch operatorString {
	case "or":
		*o = MatchQueryOperatorOr
		return nil
	case "and":
		*o = MatchQueryOperatorAnd
		return nil
	default:
		return fmt.Errorf("cannot unmarshal match operator '%s' from JSON", o)
	}
}

// NewMatchQuery creates a Query for matching text.
// An Analyzer is chosen based on the field.
// Input text is analyzed using this analyzer.
// Token terms resulting from this analysis are
// used to perform term searches.  Result documents
// must satisfy at least one of these term searches.
func NewMatchQuery(match string) *MatchQuery {
	return &MatchQuery{
		Match:       match,
		BoostVal:    1.0,
		OperatorVal: MatchQueryOperatorOr,
	}
}

// NewMatchQueryOperator creates a Query for matching text.
// An Analyzer is chosen based on the field.
// Input text is analyzed using this analyzer.
// Token terms resulting from this analysis are
// used to perform term searches.  Result documents
// must satisfy term searches according to given operator.
func NewMatchQueryOperator(match string, operator MatchQueryOperator) *MatchQuery {
	return &MatchQuery{
		Match:       match,
		BoostVal:    1.0,
		OperatorVal: operator,
	}
}

func (q *MatchQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *MatchQuery) Field() string {
	return q.FieldVal
}

func (q *MatchQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *MatchQuery) Fuzziness() int {
	return q.FuzzinessVal
}

func (q *MatchQuery) SetFuzziness(f int) Query {
	q.FuzzinessVal = f
	return q
}

func (q *MatchQuery) Prefix() int {
	return q.PrefixVal
}

func (q *MatchQuery) SetPrefix(p int) Query {
	q.PrefixVal = p
	return q
}

func (q *MatchQuery) Operator() MatchQueryOperator {
	return q.OperatorVal
}

func (q *MatchQuery) SetOperator(operator MatchQueryOperator) Query {
	q.OperatorVal = operator
	return q
}

func (q *MatchQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {

	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	analyzerName := ""
	if q.Analyzer != "" {
		analyzerName = q.Analyzer
	} else {
		analyzerName = m.AnalyzerNameForPath(field)
	}
	analyzer := m.AnalyzerNamed(analyzerName)

	if analyzer == nil {
		return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
	}

	tokens := analyzer.Analyze([]byte(q.Match))
	if len(tokens) > 0 {

		tqs := make([]Query, len(tokens))
		if q.FuzzinessVal != 0 {
			for i, token := range tokens {
				query := NewFuzzyQuery(string(token.Term))
				query.SetFuzziness(q.FuzzinessVal)
				query.SetPrefix(q.PrefixVal)
				query.SetField(field)
				query.SetBoost(q.BoostVal)
				tqs[i] = query
			}
		} else {
			for i, token := range tokens {
				tqs[i] = NewTermQuery(string(token.Term)).
					SetField(field).
					SetBoost(q.BoostVal)
			}
		}

		switch q.OperatorVal {
		case MatchQueryOperatorOr:
			shouldQuery := NewDisjunctionQueryMin(tqs, 1).
				SetBoost(q.BoostVal)

			return shouldQuery.Searcher(i, m, explain)

		case MatchQueryOperatorAnd:
			mustQuery := NewConjunctionQuery(tqs).
				SetBoost(q.BoostVal)

			return mustQuery.Searcher(i, m, explain)

		default:
			return nil, fmt.Errorf("unhandled operator %d", q.OperatorVal)
		}
	}
	noneQuery := NewMatchNoneQuery()
	return noneQuery.Searcher(i, m, explain)
}

func (q *MatchQuery) Validate() error {
	return nil
}
