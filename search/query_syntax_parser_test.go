package search

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/document"
)

func TestQuerySyntaxParserValid(t *testing.T) {

	tests := []struct {
		input   string
		result  Query
		mapping document.Mapping
	}{
		{
			input:   "test",
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test",
							Field:    "_all",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
					Min:      1.0,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   `"test phrase 1"`,
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchPhraseQuery{
							MatchPhrase: "test phrase 1",
							Field:       "_all",
							BoostVal:    1.0,
							Explain:     true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
					Min:      1.0,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   "field:test",
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test",
							Field:    "field",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
					Min:      1.0,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   "+field1:test1",
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test1",
							Field:    "field1",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   "-field2:test2",
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test2",
							Field:    "field2",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   `field3:"test phrase 2"`,
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchPhraseQuery{
							MatchPhrase: "test phrase 2",
							Field:       "field3",
							BoostVal:    1.0,
							Explain:     true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
					Min:      1.0,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   `+field4:"test phrase 1"`,
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&MatchPhraseQuery{
							MatchPhrase: "test phrase 1",
							Field:       "field4",
							BoostVal:    1.0,
							Explain:     true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   `-field5:"test phrase 2"`,
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchPhraseQuery{
							MatchPhrase: "test phrase 2",
							Field:       "field5",
							BoostVal:    1.0,
							Explain:     true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
		{
			input:   `+field6:test3 -field7:test4 field8:test5`,
			mapping: document.Mapping{},
			result: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test3",
							Field:    "field6",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test4",
							Field:    "field7",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
				},
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&MatchQuery{
							Match:    "test5",
							Field:    "field8",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					BoostVal: 1.0,
					Explain:  true,
					Min:      1.0,
				},
				BoostVal: 1.0,
				Explain:  true,
			},
		},
	}
	parsingDefaultField = "_all"
	for _, test := range tests {
		q, err := ParseQuerySyntax(test.input, test.mapping, parsingDefaultField)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(q, test.result) {
			t.Errorf("Expected %#v, got %#v: for %s", test.result, q, test.input)
			for _, x := range q.(*TermBooleanQuery).Should.Terms {
				t.Logf("term: %#v", x)
			}
		}
	}
}

func TestQuerySyntaxParserInvalid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"^"},
		{"^5"},
	}

	for _, test := range tests {
		_, err := ParseQuerySyntax(test.input, document.Mapping{}, "_all")
		if err == nil {
			t.Errorf("expected error, got nil for `%s`", test.input)
		}
	}
}
