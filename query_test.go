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
	"reflect"
	"testing"
)

var minNum = 5.1
var maxNum = 7.1
var startDate = "2011-01-01"
var endDate = "2012-01-01"

func TestParseQuery(t *testing.T) {
	tests := []struct {
		input  []byte
		output Query
		err    error
	}{
		{
			input:  []byte(`{"term":"water","field":"desc"}`),
			output: NewTermQuery("water").SetField("desc"),
		},
		{
			input:  []byte(`{"match":"beer","field":"desc"}`),
			output: NewMatchQuery("beer").SetField("desc"),
		},
		{
			input:  []byte(`{"match_phrase":"light beer","field":"desc"}`),
			output: NewMatchPhraseQuery("light beer").SetField("desc"),
		},
		{
			input: []byte(`{"must":{"terms": [{"match":"beer","field":"desc"}]},"should":{"terms": [{"match":"water","field":"desc"}]},"must_not":{"terms": [{"match":"devon","field":"desc"}]}}`),
			output: NewBooleanQuery(
				NewConjunctionQuery([]Query{NewMatchQuery("beer").SetField("desc")}),
				NewDisjunctionQuery([]Query{NewMatchQuery("water").SetField("desc")}).SetMin(0),
				NewDisjunctionQuery([]Query{NewMatchQuery("devon").SetField("desc")}).SetMin(0)),
		},
		{
			input: []byte(`{"terms":[{"term":"watered","field":"desc"},{"term":"down","field":"desc"}]}`),
			output: NewPhraseQuery([]*TermQuery{
				NewTermQuery("watered").SetField("desc"),
				NewTermQuery("down").SetField("desc")}),
		},
		{
			input:  []byte(`{"query":"+beer \"light beer\" -devon"}`),
			output: NewSyntaxQuery(`+beer "light beer" -devon`),
		},
		{
			input:  []byte(`{"min":5.1,"max":7.1,"field":"desc"}`),
			output: NewNumericRangeQuery(&minNum, &maxNum).SetField("desc"),
		},
		{
			input:  []byte(`{"start":"` + startDate + `","end":"` + endDate + `","field":"desc"}`),
			output: NewDateRangeQuery(&startDate, &endDate).SetField("desc"),
		},
		{
			input:  []byte(`{"prefix":"budwei","field":"desc"}`),
			output: NewPrefixQuery("budwei").SetField("desc"),
		},
		{
			input:  []byte(`{"madeitup":"queryhere"}`),
			output: nil,
			err:    ERROR_UNKNOWN_QUERY_TYPE,
		},
	}

	for _, test := range tests {
		actual, err := ParseQuery(test.input)
		if err != nil && test.err == nil {
			t.Error(err)
		} else if test.err != nil {
			if !reflect.DeepEqual(err, test.err) {
				t.Errorf("expected error: %#v, got: %#v", test.err, err)
			}
		}

		if !reflect.DeepEqual(test.output, actual) {
			t.Errorf("expected: %#v, got: %#v", test.output, actual)
			// t.Errorf("expected: %#v, got: %#v", test.output.(*BooleanQuery).Should, actual.(*BooleanQuery).Should)
		}
	}
}

func TestSetGetField(t *testing.T) {
	tests := []struct {
		query Query
		field string
	}{
		{
			query: NewTermQuery("water").SetField("desc"),
			field: "desc",
		},
		{
			query: NewMatchQuery("beer").SetField("desc"),
			field: "desc",
		},
		{
			query: NewMatchPhraseQuery("light beer").SetField("desc"),
			field: "desc",
		},
		{
			query: NewNumericRangeQuery(&minNum, &maxNum).SetField("desc"),
			field: "desc",
		},
		{
			query: NewDateRangeQuery(&startDate, &endDate).SetField("desc"),
			field: "desc",
		},
		{
			query: NewPrefixQuery("budwei").SetField("desc"),
			field: "desc",
		},
	}

	for _, test := range tests {
		switch query := test.query.(type) {
		case *TermQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		case *MatchQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		case *MatchPhraseQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		case *NumericRangeQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		case *DateRangeQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		case *PrefixQuery:
			if query.Field() != test.field {
				t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
			}
		}
	}
}

func TestQueryValidate(t *testing.T) {
	tests := []struct {
		query Query
		err   error
	}{
		{
			query: NewTermQuery("water").SetField("desc"),
			err:   nil,
		},
		{
			query: NewMatchQuery("beer").SetField("desc"),
			err:   nil,
		},
		{
			query: NewMatchPhraseQuery("light beer").SetField("desc"),
			err:   nil,
		},
		{
			query: NewNumericRangeQuery(&minNum, &maxNum).SetField("desc"),
			err:   nil,
		},
		{
			query: NewNumericRangeQuery(nil, nil).SetField("desc"),
			err:   ERROR_NUMERIC_QUERY_NO_BOUNDS,
		},
		{
			query: NewDateRangeQuery(&startDate, &endDate).SetField("desc"),
			err:   nil,
		},
		{
			query: NewPrefixQuery("budwei").SetField("desc"),
			err:   nil,
		},
		{
			query: NewSyntaxQuery(`+beer "light beer" -devon`),
			err:   nil,
		},
		{
			query: NewPhraseQuery([]*TermQuery{
				NewTermQuery("watered").SetField("desc"),
				NewTermQuery("down").SetField("desc")}),
			err: nil,
		},
		{
			query: NewPhraseQuery([]*TermQuery{}),
			err:   ERROR_PHRASE_QUERY_NO_TERMS,
		},
		{
			query: NewMatchNoneQuery().SetBoost(25),
			err:   nil,
		},
		{
			query: NewMatchAllQuery().SetBoost(25),
			err:   nil,
		},
		{
			query: NewBooleanQuery(
				NewConjunctionQuery([]Query{NewMatchQuery("beer").SetField("desc")}),
				NewDisjunctionQuery([]Query{NewMatchQuery("water").SetField("desc")}).SetMin(0),
				NewDisjunctionQuery([]Query{NewMatchQuery("devon").SetField("desc")}).SetMin(0)),
			err: nil,
		},
		{
			query: NewBooleanQuery(
				nil,
				nil,
				NewDisjunctionQuery([]Query{NewMatchQuery("devon").SetField("desc")}).SetMin(0)),
			err: ERROR_BOOLEAN_QUERY_NEEDS_MUST_OR_SHOULD,
		},
		{
			query: NewBooleanQuery(
				NewConjunctionQuery([]Query{}),
				NewDisjunctionQuery([]Query{}).SetMin(0),
				NewDisjunctionQuery([]Query{NewMatchQuery("devon").SetField("desc")}).SetMin(0)),
			err: ERROR_BOOLEAN_QUERY_NEEDS_MUST_OR_SHOULD,
		},
		{
			query: NewBooleanQuery(
				NewConjunctionQuery([]Query{NewMatchQuery("beer").SetField("desc")}),
				NewDisjunctionQuery([]Query{NewMatchQuery("water").SetField("desc")}).SetMin(2),
				NewDisjunctionQuery([]Query{NewMatchQuery("devon").SetField("desc")}).SetMin(0)),
			err: ERROR_DISJUNCTION_FEWER_THAN_MIN_CLAUSES,
		},
	}

	for _, test := range tests {
		actual := test.query.Validate()
		if !reflect.DeepEqual(actual, test.err) {
			t.Errorf("expected error: %#v got %#v", test.err, actual)
		}
	}
}
