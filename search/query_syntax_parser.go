package search

import (
	"fmt"
	"strings"
	"sync"

	"github.com/couchbaselabs/bleve/document"
)

var crashHard = false
var parserMutex sync.Mutex
var parsingDefaultField string
var parsingMust bool
var parsingMustNot bool
var debugParser bool
var debugLexer bool

var parsingMustList *TermConjunctionQuery
var parsingMustNotList *TermDisjunctionQuery
var parsingShouldList *TermDisjunctionQuery
var parsingMapping document.Mapping

func ParseQuerySyntax(query string, mapping document.Mapping) (rq Query, err error) {
	parserMutex.Lock()
	defer parserMutex.Unlock()

	parsingMapping = mapping

	parsingMustList = &TermConjunctionQuery{
		Terms:    make([]Query, 0),
		BoostVal: 1.0,
		Explain:  true,
	}

	parsingMustNotList = &TermDisjunctionQuery{
		Terms:    make([]Query, 0),
		BoostVal: 1.0,
		Explain:  true,
	}

	parsingShouldList = &TermDisjunctionQuery{
		Terms:    make([]Query, 0),
		BoostVal: 1.0,
		Explain:  true,
		Min:      1,
	}

	defer func() {
		r := recover()
		if r != nil && r == "syntax error" {
			// if we're panicing over a syntax error, chill
			err = fmt.Errorf("Parse Error - %v", r)
		} else if r != nil {
			// otherise continue to panic
			if crashHard {
				panic(r)
			} else {
				err = fmt.Errorf("Other Error - %v", r)
			}
		}
	}()

	yyParse(NewLexer(strings.NewReader(query)))
	parsingQuery := &TermBooleanQuery{
		BoostVal: 1.0,
		Explain:  true,
	}
	if len(parsingMustList.Terms) > 0 {
		parsingQuery.Must = parsingMustList
	}
	if len(parsingMustNotList.Terms) > 0 {
		parsingQuery.MustNot = parsingMustNotList
	}
	if len(parsingShouldList.Terms) > 0 {
		parsingQuery.Should = parsingShouldList
	}
	rq = parsingQuery
	return rq, err
}
