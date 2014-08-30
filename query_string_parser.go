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
var parsingMust bool
var parsingMustNot bool
var debugParser bool
var debugLexer bool

var parsingLastQuery Query
var parsingMustList []Query
var parsingMustNotList []Query
var parsingShouldList []Query
var parsingIndexMapping *IndexMapping

func parseQuerySyntax(query string, mapping *IndexMapping) (rq Query, err error) {
	parserMutex.Lock()
	defer parserMutex.Unlock()

	parsingIndexMapping = mapping
	parsingMustList = make([]Query, 0)
	parsingMustNotList = make([]Query, 0)
	parsingShouldList = make([]Query, 0)

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

	yyParse(newLexer(strings.NewReader(query)))
	rq = NewBooleanQuery(parsingMustList, parsingShouldList, parsingMustNotList)
	return rq, err
}

func addQueryToList(q Query) {
	if parsingMust {
		parsingMustList = append(parsingMustList, q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList = append(parsingMustNotList, q)
		parsingMustNot = false
	} else {
		parsingShouldList = append(parsingShouldList, q)
	}
	parsingLastQuery = q
}
