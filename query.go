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

	"github.com/blevesearch/bleve/search"
)

type Query interface {
	Boost() float64
	Searcher(i *indexImpl, explain bool) (search.Searcher, error)
	Validate() error
}

func ParseQuery(input []byte) (Query, error) {
	var tmp map[string]interface{}
	err := json.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}
	_, isTermQuery := tmp["term"]
	if isTermQuery {
		var rv TermQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	_, isMatchQuery := tmp["match"]
	if isMatchQuery {
		var rv MatchQuery
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
		var rv MatchPhraseQuery
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
		var rv BooleanQuery
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
		var rv PhraseQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		for _, tq := range rv.Terms {
			if tq.Boost() == 0 {
				tq.SetBoost(1)
			}
		}
		return &rv, nil
	}
	_, hasSyntaxQuery := tmp["query"]
	if hasSyntaxQuery {
		var rv QueryStringQuery
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
		var rv NumericRangeQuery
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
		var rv DateRangeQuery
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
		var rv PrefixQuery
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		if rv.Boost() == 0 {
			rv.SetBoost(1)
		}
		return &rv, nil
	}
	return nil, ERROR_UNKNOWN_QUERY_TYPE
}
