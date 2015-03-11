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
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

var wildcardRegexpReplacer = strings.NewReplacer(
	// characters in the wildcard that must
	// be escaped in the regexp
	"+", `\+`,
	"(", `\(`,
	")", `\)`,
	"^", `\^`,
	"$", `\$`,
	".", `\.`,
	"{", `\{`,
	"}", `\}`,
	"[", `\[`,
	"]", `\]`,
	`|`, `\|`,
	`\`, `\\`,
	// wildcard characters
	"*", ".*",
	"?", ".")

type wildcardQuery struct {
	Wildcard string  `json:"wildcard"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
	compiled *regexp.Regexp
}

// NewWildcardQuery creates a new Query which finds
// documents containing terms that match the
// specified wildcard.  In the wildcard pattern '*'
// will match any sequence of 0 or more characters,
// and '?' will match any single character.
func NewWildcardQuery(wildcard string) *wildcardQuery {
	return &wildcardQuery{
		Wildcard: wildcard,
		BoostVal: 1.0,
	}
}

func (q *wildcardQuery) Boost() float64 {
	return q.BoostVal
}

func (q *wildcardQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *wildcardQuery) Field() string {
	return q.FieldVal
}

func (q *wildcardQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *wildcardQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	if q.compiled == nil {
		var err error
		q.compiled, err = q.convertToRegexp()
		if err != nil {
			return nil, err
		}
	}

	return searchers.NewRegexpSearcher(i, q.compiled, field, q.BoostVal, explain)
}

func (q *wildcardQuery) Validate() error {
	var err error
	q.compiled, err = q.convertToRegexp()
	return err
}

func (q *wildcardQuery) convertToRegexp() (*regexp.Regexp, error) {
	regexpString := "^" + wildcardRegexpReplacer.Replace(q.Wildcard) + "$"
	return regexp.Compile(regexpString)
}
