package query

import (
	"reflect"
	"strings"
	"testing"
)

func TestLexer(t *testing.T) {

	tests := []struct {
		input  string
		tokens []token
	}{
		{
			input: "test",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test",
					},
				},
			},
		},
		{
			input: "127.0.0.1",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "127.0.0.1",
					},
				},
			},
		},
		{
			input: `"test phrase 1"`,
			tokens: []token{
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "test phrase 1",
					},
				},
			},
		},
		{
			input: "field:test",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test",
					},
				},
			},
		},
		{
			input: "field:t-est",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "t-est",
					},
				},
			},
		},
		{
			input: "field:t+est",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "t+est",
					},
				},
			},
		},
		{
			input: "field:t>est",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "t>est",
					},
				},
			},
		},
		{
			input: "field:t<est",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ:  tCOLON,
					lval: yySymType{},
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "t<est",
					},
				},
			},
		},

		{
			input: "field:t=est",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "t=est",
					},
				},
			},
		},
		{
			input: "+field1:test1",
			tokens: []token{
				{
					typ: tPLUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field1",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test1",
					},
				},
			},
		},
		{
			input: "-field2:test2",
			tokens: []token{
				{
					typ: tMINUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field2",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test2",
					},
				},
			},
		},
		{
			input: `field3:"test phrase 2"`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field3",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "test phrase 2",
					},
				},
			},
		},
		{
			input: `+field4:"test phrase 1"`,
			tokens: []token{
				{
					typ: tPLUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field4",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "test phrase 1",
					},
				},
			},
		},
		{
			input: `-field5:"test phrase 2"`,
			tokens: []token{
				{
					typ: tMINUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field5",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "test phrase 2",
					},
				},
			},
		},
		{
			input: `+field6:test3 -field7:test4 field8:test5`,
			tokens: []token{
				{
					typ: tPLUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field6",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test3",
					},
				},
				{
					typ: tMINUS,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field7",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test4",
					},
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field8",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test5",
					},
				},
			},
		},
		{
			input: "test^3",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "3",
					},
				},
			},
		},
		{
			input: "test^3 other^6",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "test",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "3",
					},
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "other",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "6",
					},
				},
			},
		},
		{
			input: "33",
			tokens: []token{
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "33",
					},
				},
			},
		},
		{
			input: "field:33",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "33",
					},
				},
			},
		},
		{
			input: "cat-dog",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "cat-dog",
					},
				},
			},
		},
		{
			input: "watex~",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "watex",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "1",
					},
				},
			},
		},
		{
			input: "watex~2",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "watex",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "2",
					},
				},
			},
		},
		{
			input: "watex~ 2",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "watex",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "1",
					},
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "2",
					},
				},
			},
		},
		{
			input: "field:watex~",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "watex",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "1",
					},
				},
			},
		},
		{
			input: "field:watex~2",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "watex",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "2",
					},
				},
			},
		},
		{
			input: `field:555c3bb06f7a127cda000005`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "555c3bb06f7a127cda000005",
					},
				},
			},
		},
		{
			input: `field:>5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:>=5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:<5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:<=5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: "field:-5",
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tMINUS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:>-5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tMINUS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:>=-5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tMINUS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:<-5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tMINUS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:<=-5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tMINUS,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `field:>"2006-01-02T15:04:05Z"`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "2006-01-02T15:04:05Z",
					},
				},
			},
		},
		{
			input: `field:>="2006-01-02T15:04:05Z"`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tGREATER,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "2006-01-02T15:04:05Z",
					},
				},
			},
		},
		{
			input: `field:<"2006-01-02T15:04:05Z"`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "2006-01-02T15:04:05Z",
					},
				},
			},
		},
		{
			input: `field:<="2006-01-02T15:04:05Z"`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "field",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tLESS,
				},
				{
					typ: tEQUAL,
				},
				{
					typ: tPHRASE,
					lval: yySymType{
						s: "2006-01-02T15:04:05Z",
					},
				},
			},
		},
		{
			input: `/mar.*ty/`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `/mar.*ty/`,
					},
				},
			},
		},
		{
			input: `name:/mar.*ty/`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "name",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: `/mar.*ty/`,
					},
				},
			},
		},
		{
			input: `mart*`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `mart*`,
					},
				},
			},
		},
		{
			input: `name:mart*`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "name",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: `mart*`,
					},
				},
			},
		},
		{
			input: `name\:marty`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `name:marty`,
					},
				},
			},
		},
		{
			input: `name:marty\:couchbase`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "name",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: `marty:couchbase`,
					},
				},
			},
		},
		{
			input: `marty\ couchbase`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `marty couchbase`,
					},
				},
			},
		},
		{
			input: `\+marty`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `+marty`,
					},
				},
			},
		},
		{
			input: `\-marty`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `-marty`,
					},
				},
			},
		},
		{
			input: `"what does \"quote\" mean"`,
			tokens: []token{
				{
					typ: tPHRASE,
					lval: yySymType{
						s: `what does "quote" mean`,
					},
				},
			},
		},
		{
			input: `can\ i\ escap\e`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `can i escap\e`,
					},
				},
			},
		},
		{
			input: `   what`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `what`,
					},
				},
			},
		},
		{
			input: `term^`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `term`,
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "1",
					},
				},
			},
		},
		{
			input: `3.0\:`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `3.0:`,
					},
				},
			},
		},
		{
			input: `3.0\a`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: `3.0\a`,
					},
				},
			},
		},
		{
			input: `age:65^10`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "age",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "65",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "10",
					},
				},
			},
		},
		{
			input: `age:65^10 age:18^5`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "age",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "65",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "10",
					},
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "age",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "18",
					},
				},
				{
					typ: tBOOST,
					lval: yySymType{
						s: "5",
					},
				},
			},
		},
		{
			input: `age:65~2`,
			tokens: []token{
				{
					typ: tSTRING,
					lval: yySymType{
						s: "age",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "65",
					},
				},
				{
					typ: tTILDE,
					lval: yySymType{
						s: "2",
					},
				},
			},
		},
		{
			input: `65:cat`,
			tokens: []token{
				{
					typ: tNUMBER,
					lval: yySymType{
						s: "65",
					},
				},
				{
					typ: tCOLON,
				},
				{
					typ: tSTRING,
					lval: yySymType{
						s: "cat",
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.input, func(t *testing.T) {

			r := strings.NewReader(test.input)
			l := newQueryStringLex(r)
			var tokens []token
			var lval yySymType
			rv := l.Lex(&lval)
			for rv > 0 {
				//tokenTypes = append(tokenTypes, rv)
				tokens = append(tokens, token{typ: rv, lval: lval})
				lval.s = ""
				lval.n = 0
				rv = l.Lex(&lval)
			}

			if !reflect.DeepEqual(tokens, test.tokens) {
				t.Fatalf("\nexpected: %#v\n     got: %#v\n", test.tokens, tokens)
			}
		})
	}
}

type token struct {
	typ  int
	lval yySymType
}
