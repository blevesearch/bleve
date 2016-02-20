//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type FuzzySearcher struct {
	indexReader index.IndexReader
	term        string
	prefix      int
	fuzziness   int
	field       string
	explain     bool
	searcher    *DisjunctionSearcher
}

func NewFuzzySearcher(indexReader index.IndexReader, term string, prefix, fuzziness int, field string, boost float64, explain bool) (*FuzzySearcher, error) {
	prefixTerm := ""
	for i, r := range term {
		if i < prefix {
			prefixTerm += string(r)
		}
	}

	// find the terms with this prefix
	var fieldDict index.FieldDict
	var err error
	if len(prefixTerm) > 0 {
		fieldDict, err = indexReader.FieldDictPrefix(field, []byte(prefixTerm))
	} else {
		fieldDict, err = indexReader.FieldDict(field)
	}

	// enumerate terms and check levenshtein distance
	candidateTerms := make([]string, 0)
	tfd, err := fieldDict.Next()
	for err == nil && tfd != nil {
		ld, exceeded := search.LevenshteinDistanceMax(&term, &tfd.Term, fuzziness)
		if !exceeded && ld <= fuzziness {
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

	return &FuzzySearcher{
		indexReader: indexReader,
		term:        term,
		prefix:      prefix,
		fuzziness:   fuzziness,
		field:       field,
		explain:     explain,
		searcher:    searcher,
	}, nil
}
func (s *FuzzySearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *FuzzySearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *FuzzySearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *FuzzySearcher) Next() (*search.DocumentMatch, error) {
	return s.searcher.Next()

}

func (s *FuzzySearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return s.searcher.Next()
}

func (s *FuzzySearcher) Close() error {
	return s.searcher.Close()
}

func (s *FuzzySearcher) Min() int {
	return 0
}
