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

type TermPrefixSearcher struct {
	index    index.Index
	prefix   string
	field    string
	explain  bool
	searcher *DisjunctionSearcher
}

func NewTermPrefixSearcher(index index.Index, prefix string, field string, boost float64, explain bool) (*TermPrefixSearcher, error) {
	// find the terms with this prefix
	fieldReader, err := index.FieldReader(field, []byte(prefix), []byte(prefix))

	// enumerate all the terms in the range
	qsearchers := make([]search.Searcher, 0, 25)
	tfd, err := fieldReader.Next()
	for err == nil && tfd != nil {
		qsearcher, err := NewTermSearcher(index, string(tfd.Term), field, 1.0, explain)
		if err != nil {
			return nil, err
		}
		qsearchers = append(qsearchers, qsearcher)
		tfd, err = fieldReader.Next()
	}
	// build disjunction searcher of these ranges
	searcher, err := NewDisjunctionSearcher(index, qsearchers, 0, explain)
	if err != nil {
		return nil, err
	}

	return &TermPrefixSearcher{
		index:    index,
		prefix:   prefix,
		field:    field,
		explain:  explain,
		searcher: searcher,
	}, nil
}
func (s *TermPrefixSearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *TermPrefixSearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *TermPrefixSearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *TermPrefixSearcher) Next() (*search.DocumentMatch, error) {
	return s.searcher.Next()

}

func (s *TermPrefixSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return s.searcher.Next()
}

func (s *TermPrefixSearcher) Close() {
	s.searcher.Close()
}

func (s *TermPrefixSearcher) Min() int {
	return 0
}
