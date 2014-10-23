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
	fieldReader, err := indexReader.FieldReader(field, []byte(prefixTerm), []byte(prefixTerm))

	// enumerate terms and check levenshtein distance
	candidateTerms := make([]string, 0)
	tfd, err := fieldReader.Next()
	for err == nil && tfd != nil {
		ld := levenshteinDistance(&term, &tfd.Term)
		if ld <= fuzziness {
			candidateTerms = append(candidateTerms, tfd.Term)
		}
		tfd, err = fieldReader.Next()
	}

	// enumerate all the terms in the range
	qsearchers := make([]search.Searcher, 0, 25)

	for _, cterm := range candidateTerms {
		qsearcher, err := NewTermSearcher(indexReader, cterm, field, 1.0, explain)
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

func (s *FuzzySearcher) Close() {
	s.searcher.Close()
}

func (s *FuzzySearcher) Min() int {
	return 0
}

func levenshteinDistance(a, b *string) int {
	la := len(*a)
	lb := len(*b)
	d := make([]int, la+1)
	var lastdiag, olddiag, temp int

	for i := 1; i <= la; i++ {
		d[i] = i
	}
	for i := 1; i <= lb; i++ {
		d[0] = i
		lastdiag = i - 1
		for j := 1; j <= la; j++ {
			olddiag = d[j]
			min := d[j] + 1
			if (d[j-1] + 1) < min {
				min = d[j-1] + 1
			}
			if (*a)[j-1] == (*b)[i-1] {
				temp = 0
			} else {
				temp = 1
			}
			if (lastdiag + temp) < min {
				min = lastdiag + temp
			}
			d[j] = min
			lastdiag = olddiag
		}
	}
	return d[la]
}
