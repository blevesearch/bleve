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
	"regexp"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type RegexpSearcher struct {
	indexReader index.IndexReader
	pattern     *regexp.Regexp
	field       string
	explain     bool
	searcher    *DisjunctionSearcher
}

func NewRegexpSearcher(indexReader index.IndexReader, pattern *regexp.Regexp, field string, boost float64, explain bool) (*RegexpSearcher, error) {

	prefixTerm, complete := pattern.LiteralPrefix()
	candidateTerms := make([]string, 0)
	if complete {
		// there is no pattern
		candidateTerms = append(candidateTerms, prefixTerm)
	} else {
		var fieldDict index.FieldDict
		var err error
		if len(prefixTerm) > 0 {
			fieldDict, err = indexReader.FieldDictPrefix(field, []byte(prefixTerm))
		} else {
			fieldDict, err = indexReader.FieldDict(field)
		}

		// enumerate the terms and check against regexp
		tfd, err := fieldDict.Next()
		for err == nil && tfd != nil {
			if pattern.MatchString(tfd.Term) {
				candidateTerms = append(candidateTerms, tfd.Term)
			}
			tfd, err = fieldDict.Next()
		}
		if err != nil {
			return nil, err
		}

		err = fieldDict.Close()
		if err != nil {
			return nil, err
		}
	}

	// enumerate all the terms in the range
	qsearchers := make([]search.Searcher, 0, 25)

	for _, cterm := range candidateTerms {
		qsearcher, err := NewTermSearcher(indexReader, cterm, field, boost, explain)
		if err != nil {
			return nil, err
		}
		qsearchers = append(qsearchers, qsearcher)
	}

	// build disjunction searcher of these ranges
	searcher, err := NewDisjunctionSearcher(indexReader, qsearchers, 0, explain)
	if err != nil {
		return nil, err
	}

	return &RegexpSearcher{
		indexReader: indexReader,
		pattern:     pattern,
		field:       field,
		explain:     explain,
		searcher:    searcher,
	}, nil
}
func (s *RegexpSearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *RegexpSearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *RegexpSearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *RegexpSearcher) Next() (*search.DocumentMatch, error) {
	return s.searcher.Next()

}

func (s *RegexpSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return s.searcher.Next()
}

func (s *RegexpSearcher) Close() error {
	return s.searcher.Close()
}

func (s *RegexpSearcher) Min() int {
	return 0
}
