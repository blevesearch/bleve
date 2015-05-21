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

func TestQuerySyntaxParserValid(t *testing.T) {
	fivePointOh := 5.0
	theTruth := true
	theFalsehood := false
	tests := []struct {
		input   string
		result  Query
		mapping *IndexMapping
	}{
		{
			input:   "test",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("test"),
				},
				nil),
		},
		{
			input:   `"test phrase 1"`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchPhraseQuery("test phrase 1"),
				},
				nil),
		},
		{
			input:   "field:test",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("test").SetField("field"),
				},
				nil),
		},
		// - is allowed inside a term, just not the start
		{
			input:   "field:t-est",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("t-est").SetField("field"),
				},
				nil),
		},
		// + is allowed inside a term, just not the start
		{
			input:   "field:t+est",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("t+est").SetField("field"),
				},
				nil),
		},
		// > is allowed inside a term, just not the start
		{
			input:   "field:t>est",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("t>est").SetField("field"),
				},
				nil),
		},
		// < is allowed inside a term, just not the start
		{
			input:   "field:t<est",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("t<est").SetField("field"),
				},
				nil),
		},
		// = is allowed inside a term, just not the start
		{
			input:   "field:t=est",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("t=est").SetField("field"),
				},
				nil),
		},
		{
			input:   "+field1:test1",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				[]Query{
					NewMatchQuery("test1").SetField("field1"),
				},
				nil,
				nil),
		},
		{
			input:   "-field2:test2",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				nil,
				[]Query{
					NewMatchQuery("test2").SetField("field2"),
				}),
		},
		{
			input:   `field3:"test phrase 2"`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchPhraseQuery("test phrase 2").SetField("field3"),
				},
				nil),
		},
		{
			input:   `+field4:"test phrase 1"`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				[]Query{
					NewMatchPhraseQuery("test phrase 1").SetField("field4"),
				},
				nil,
				nil),
		},
		{
			input:   `-field5:"test phrase 2"`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				nil,
				[]Query{
					NewMatchPhraseQuery("test phrase 2").SetField("field5"),
				}),
		},
		{
			input:   `+field6:test3 -field7:test4 field8:test5`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				[]Query{
					NewMatchQuery("test3").SetField("field6"),
				},
				[]Query{
					NewMatchQuery("test5").SetField("field8"),
				},
				[]Query{
					NewMatchQuery("test4").SetField("field7"),
				}),
		},
		{
			input:   "test^3",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("test").SetBoost(3.0),
				},
				nil),
		},
		{
			input:   "test^3 other^6",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("test").SetBoost(3.0),
					NewMatchQuery("other").SetBoost(6.0),
				},
				nil),
		},
		{
			input:   "33",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("33"),
				},
				nil),
		},
		{
			input:   "field:33",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("33").SetField("field"),
				},
				nil),
		},
		{
			input:   "cat-dog",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("cat-dog"),
				},
				nil),
		},
		{
			input:   "watex~",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("watex").SetFuzziness(1),
				},
				nil),
		},
		{
			input:   "watex~2",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("watex").SetFuzziness(2),
				},
				nil),
		},
		{
			input:   "watex~ 2",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("watex").SetFuzziness(1),
					NewMatchQuery("2"),
				},
				nil),
		},
		{
			input:   "field:watex~",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("watex").SetFuzziness(1).SetField("field"),
				},
				nil),
		},
		{
			input:   "field:watex~2",
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("watex").SetFuzziness(2).SetField("field"),
				},
				nil),
		},
		{
			input:   `field:555c3bb06f7a127cda000005`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewMatchQuery("555c3bb06f7a127cda000005").SetField("field"),
				},
				nil),
		},
		{
			input:   `field:>5`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewNumericRangeInclusiveQuery(&fivePointOh, nil, &theFalsehood, nil).SetField("field"),
				},
				nil),
		},
		{
			input:   `field:>=5`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewNumericRangeInclusiveQuery(&fivePointOh, nil, &theTruth, nil).SetField("field"),
				},
				nil),
		},
		{
			input:   `field:<5`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewNumericRangeInclusiveQuery(nil, &fivePointOh, nil, &theFalsehood).SetField("field"),
				},
				nil),
		},
		{
			input:   `field:<=5`,
			mapping: NewIndexMapping(),
			result: NewBooleanQuery(
				nil,
				[]Query{
					NewNumericRangeInclusiveQuery(nil, &fivePointOh, nil, &theTruth).SetField("field"),
				},
				nil),
		},
	}

	// turn on lexer debugging
	// debugLexer = true
	// logger = log.New(os.Stderr, "bleve", log.LstdFlags)

	for _, test := range tests {

		q, err := parseQuerySyntax(test.input, test.mapping)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(q, test.result) {
			t.Errorf("Expected %#v, got %#v: for %s", test.result, q, test.input)
			t.Errorf("Expected %#v, got %#v: for %s", test.result.(*booleanQuery).Should.(*disjunctionQuery).Disjuncts[0], q.(*booleanQuery).Should.(*disjunctionQuery).Disjuncts[0], test.input)
		}
	}
}

func TestQuerySyntaxParserInvalid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"^"},
		{"^5"},
		{"field:-text"},
		{"field:+text"},
		{"field:>text"},
		{"field:>=text"},
		{"field:<text"},
		{"field:<=text"},
		{"field:~text"},
		{"field:^text"},
		{"field::text"},
	}

	// turn on lexer debugging
	// debugLexer = true
	// logger = log.New(os.Stderr, "bleve", log.LstdFlags)

	for _, test := range tests {
		_, err := parseQuerySyntax(test.input, NewIndexMapping())
		if err == nil {
			t.Errorf("expected error, got nil for `%s`", test.input)
		}
	}
}
