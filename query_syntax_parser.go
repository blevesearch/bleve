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
	"strings"
	"sync"
)

var crashHard = false
var parserMutex sync.Mutex
var parsingDefaultField string
var parsingMust bool
var parsingMustNot bool
var debugParser bool
var debugLexer bool

var parsingMustList *ConjunctionQuery
var parsingMustNotList *DisjunctionQuery
var parsingShouldList *DisjunctionQuery
var parsingIndexMapping *IndexMapping

func ParseQuerySyntax(query string, mapping *IndexMapping, defaultField string) (rq Query, err error) {
	parserMutex.Lock()
	defer parserMutex.Unlock()

	parsingIndexMapping = mapping
	parsingDefaultField = defaultField
	parsingMustList = NewConjunctionQuery([]Query{})
	parsingMustNotList = NewDisjunctionQuery([]Query{})
	parsingShouldList = NewDisjunctionQuery([]Query{})

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
	parsingQuery := NewBooleanQuery(nil, nil, nil)
	if len(parsingMustList.Conjuncts) > 0 {
		parsingQuery.Must = parsingMustList
	}
	if len(parsingMustNotList.Disjuncts) > 0 {
		parsingQuery.MustNot = parsingMustNotList
	}
	if len(parsingShouldList.Disjuncts) > 0 {
		parsingQuery.Should = parsingShouldList
	}
	rq = parsingQuery
	return rq, err
}
