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
	"strings"
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
			input:  []byte(`{"match":"beer","field":"desc","operator":"or"}`),
			output: NewMatchQuery("beer").SetField("desc"),
		},
		{
			input:  []byte(`{"match":"beer","field":"desc","operator":"and"}`),
			output: NewMatchQueryOperator("beer", MatchQueryOperatorAnd).SetField("desc"),
		},
		{
			input:  []byte(`{"match":"beer","field":"desc","operator":"or"}`),
			output: NewMatchQueryOperator("beer", MatchQueryOperatorOr).SetField("desc"),
		},
		{
			input:  []byte(`{"match":"beer","field":"desc","operator":"does not exist"}`),
			output: nil,
			err:    matchQueryOperatorUnmarshalError("does not exist"),
		},
		{
			input:  []byte(`{"match_phrase":"light beer","field":"desc"}`),
			output: NewMatchPhraseQuery("light beer").SetField("desc"),
		},
		{
			input: []byte(`{"must":{"conjuncts": [{"match":"beer","field":"desc"}]},"should":{"disjuncts": [{"match":"water","field":"desc"}],"min":1.0},"must_not":{"disjuncts": [{"match":"devon","field":"desc"}]}}`),
			output: NewBooleanQueryMinShould(
				[]Query{NewMatchQuery("beer").SetField("desc")},
				[]Query{NewMatchQuery("water").SetField("desc")},
				[]Query{NewMatchQuery("devon").SetField("desc")},
				1.0),
		},
		{
			input:  []byte(`{"terms":["watered","down"],"field":"desc"}`),
			output: NewPhraseQuery([]string{"watered", "down"}, "desc"),
		},
		{
			input:  []byte(`{"query":"+beer \"light beer\" -devon"}`),
			output: NewQueryStringQuery(`+beer "light beer" -devon`),
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
			input:  []byte(`{"match_all":{}}`),
			output: NewMatchAllQuery(),
		},
		{
			input:  []byte(`{"match_none":{}}`),
			output: NewMatchNoneQuery(),
		},
		{
			input:  []byte(`{"ids":["a","b","c"]}`),
			output: NewDocIDQuery([]string{"a", "b", "c"}),
		},
		{
			input:  []byte(`{"madeitup":"queryhere"}`),
			output: nil,
			err:    ErrorUnknownQueryType,
		},
	}

	for i, test := range tests {
		actual, err := ParseQuery(test.input)
		if err != nil && test.err == nil {
			t.Errorf("error %v for %d", err, i)
		} else if test.err != nil {
			if !reflect.DeepEqual(err, test.err) {
				t.Errorf("expected error: %#v, got: %#v", test.err, err)
			}
		}

		if !reflect.DeepEqual(test.output, actual) {
			t.Errorf("expected: %#v, got: %#v", test.output, actual)
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
		query := test.query
		if query.Field() != test.field {
			t.Errorf("expected field '%s', got '%s'", test.field, query.Field())
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
			err:   ErrorNumericQueryNoBounds,
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
			query: NewQueryStringQuery(`+beer "light beer" -devon`),
			err:   nil,
		},
		{
			query: NewPhraseQuery([]string{"watered", "down"}, "desc"),
			err:   nil,
		},
		{
			query: NewPhraseQuery([]string{}, "field"),
			err:   ErrorPhraseQueryNoTerms,
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
				[]Query{NewMatchQuery("beer").SetField("desc")},
				[]Query{NewMatchQuery("water").SetField("desc")},
				[]Query{NewMatchQuery("devon").SetField("desc")}),
			err: nil,
		},
		{
			query: NewBooleanQuery(
				nil,
				nil,
				[]Query{NewMatchQuery("devon").SetField("desc")}),
			err: nil,
		},
		{
			query: NewBooleanQuery(
				[]Query{},
				[]Query{},
				[]Query{NewMatchQuery("devon").SetField("desc")}),
			err: nil,
		},
		{
			query: NewBooleanQuery(
				nil,
				nil,
				nil),
			err: ErrorBooleanQueryNeedsMustOrShouldOrNotMust,
		},
		{
			query: NewBooleanQuery(
				[]Query{},
				[]Query{},
				[]Query{}),
			err: ErrorBooleanQueryNeedsMustOrShouldOrNotMust,
		},
		{
			query: NewBooleanQueryMinShould(
				[]Query{NewMatchQuery("beer").SetField("desc")},
				[]Query{NewMatchQuery("water").SetField("desc")},
				[]Query{NewMatchQuery("devon").SetField("desc")},
				2.0),
			err: ErrorDisjunctionFewerThanMinClauses,
		},
		{
			query: NewDocIDQuery(nil).SetBoost(25),
			err:   nil,
		},
	}

	for _, test := range tests {
		actual := test.query.Validate()
		if !reflect.DeepEqual(actual, test.err) {
			t.Errorf("expected error: %#v got %#v", test.err, actual)
		}
	}
}

func TestDumpQuery(t *testing.T) {
	mapping := &IndexMapping{}
	q := NewQueryStringQuery("+water -light beer")
	s, err := DumpQuery(mapping, q)
	if err != nil {
		t.Fatal(err)
	}
	s = strings.TrimSpace(s)
	wanted := strings.TrimSpace(`{
  "must": {
    "conjuncts": [
      {
        "match": "water",
        "boost": 1,
        "prefix_length": 0,
        "fuzziness": 0
      }
    ],
    "boost": 1
  },
  "should": {
    "disjuncts": [
      {
        "match": "beer",
        "boost": 1,
        "prefix_length": 0,
        "fuzziness": 0
      }
    ],
    "boost": 1,
    "min": 0
  },
  "must_not": {
    "disjuncts": [
      {
        "match": "light",
        "boost": 1,
        "prefix_length": 0,
        "fuzziness": 0
      }
    ],
    "boost": 1,
    "min": 0
  },
  "boost": 1
}`)
	if wanted != s {
		t.Fatalf("query:\n%s\ndiffers from expected:\n%s", s, wanted)
	}
}
