//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"sort"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

// DocIDSearcher returns documents matching a predefined set of identifiers.
type DocIDSearcher struct {
	ids     []string
	current int
	scorer  *scorers.ConstantScorer
}

func NewDocIDSearcher(indexReader index.IndexReader, ids []string, boost float64,
	explain bool) (searcher *DocIDSearcher, err error) {

	kept := make([]string, len(ids))
	copy(kept, ids)
	sort.Strings(kept)

	if len(ids) > 0 {
		var idReader index.DocIDReader
		endTerm := string(incrementBytes([]byte(kept[len(kept)-1])))
		idReader, err = indexReader.DocIDReader(kept[0], endTerm)
		if err != nil {
			return nil, err
		}
		defer func() {
			if cerr := idReader.Close(); err == nil && cerr != nil {
				err = cerr
			}
		}()
		j := 0
		for _, id := range kept {
			doc, err := idReader.Advance(id)
			if err != nil {
				return nil, err
			}
			// Non-duplicate match
			if doc == id && (j == 0 || kept[j-1] != id) {
				kept[j] = id
				j++
			}
		}
		kept = kept[:j]
	}

	scorer := scorers.NewConstantScorer(1.0, boost, explain)
	return &DocIDSearcher{
		ids:    kept,
		scorer: scorer,
	}, nil
}

func (s *DocIDSearcher) Count() uint64 {
	return uint64(len(s.ids))
}

func (s *DocIDSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *DocIDSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *DocIDSearcher) Next() (*search.DocumentMatch, error) {
	if s.current >= len(s.ids) {
		return nil, nil
	}
	id := s.ids[s.current]
	s.current++
	docMatch := s.scorer.Score(id)
	return docMatch, nil

}

func (s *DocIDSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	s.current = sort.SearchStrings(s.ids, ID)
	return s.Next()
}

func (s *DocIDSearcher) Close() error {
	return nil
}

func (s *DocIDSearcher) Min() int {
	return 0
}
