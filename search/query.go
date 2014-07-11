//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
)

type Query interface {
	Boost() float64
	Searcher(index index.Index) (Searcher, error)
	Validate() error
}

func ParseQuery(input []byte, mapping document.Mapping) (Query, error) {
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
		return &rv, nil
	}
	_, isMatchQuery := tmp["match"]
	if isMatchQuery {
		log.Printf("detected match query")
		var rv MatchQuery
		rv.mapping = mapping
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, isMatchPhraseQuery := tmp["match_phrase"]
	if isMatchPhraseQuery {
		log.Printf("detected match phrase query")
		var rv MatchPhraseQuery
		rv.mapping = mapping
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	_, hasMust := tmp["must"]
	_, hasShould := tmp["should"]
	_, hasMustNot := tmp["must_not"]
	if hasMust || hasShould || hasMustNot {
		var rv TermBooleanQuery
		rv.mapping = mapping
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
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
		return &rv, nil
	}
	_, hasSyntaxQuery := tmp["query"]
	if hasSyntaxQuery {
		var rv SyntaxQuery
		rv.mapping = mapping
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil
	}
	return nil, fmt.Errorf("Unrecognized query")
}
