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
	"encoding/json"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

// A Query represents a description of the type
// and parameters for a query into the index.
type Query interface {
	Boost() float64
	SetBoost(b float64) Query
	Field() string
	SetField(f string) Query
	Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error)
	Validate() error
}

// ParseQuery deserializes a JSON representation of
// a Query object.
func ParseQuery(input []byte) (Query, error) {
	var tmp map[string]interface{}
	err := json.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}
	_, isMatchQuery := tmp["match"]
	_, hasFuzziness := tmp["fuzziness"]
	if hasFuzziness && !isMatchQuery {
		var rv fuzzyQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, isTermQuery := tmp["term"]
	if isTermQuery {
		var rv termQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	if isMatchQuery {
		var rv matchQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, isMatchPhraseQuery := tmp["match_phrase"]
	if isMatchPhraseQuery {
		var rv matchPhraseQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasMust := tmp["must"]
	_, hasShould := tmp["should"]
	_, hasMustNot := tmp["must_not"]
	if hasMust || hasShould || hasMustNot {
		var rv booleanQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasTerms := tmp["terms"]
	if hasTerms {
		var rv phraseQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		for _, tq := range rv.TermQueries {
			if tq.Boost() == 0 {
				tq.SetBoost(1)
			}
		}
		return &rv, nil
	}
	_, hasConjuncts := tmp["conjuncts"]
	if hasConjuncts {
		var rv conjunctionQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasDisjuncts := tmp["disjuncts"]
	if hasDisjuncts {
		var rv disjunctionQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}

	_, hasSyntaxQuery := tmp["query"]
	if hasSyntaxQuery {
		var rv queryStringQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasMin := tmp["min"]
	_, hasMax := tmp["max"]
	if hasMin || hasMax {
		var rv numericRangeQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasStart := tmp["start"]
	_, hasEnd := tmp["end"]
	if hasStart || hasEnd {
		var rv dateRangeQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasPrefix := tmp["prefix"]
	if hasPrefix {
		var rv prefixQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, hasRegexp := tmp["regexp"]
	if hasRegexp {
		var rv regexpQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasWildcard := tmp["wildcard"]
	if hasWildcard {
		var rv wildcardQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	return nil, ErrorUnknownQueryType
}
