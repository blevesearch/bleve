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
	"github.com/couchbaselabs/bleve/index"
)

type TermSearcher struct {
	index  index.Index
	query  *TermQuery
	reader index.TermFieldReader
	scorer *TermQueryScorer
}

func NewTermSearcher(index index.Index, query *TermQuery) (*TermSearcher, error) {
	reader, err := index.TermFieldReader([]byte(query.Term), query.Field)
	if err != nil {
		return nil, err
	}
	scorer := NewTermQueryScorer(query, index.DocCount(), reader.Count(), query.Explain)
	return &TermSearcher{
		index:  index,
		query:  query,
		reader: reader,
		scorer: scorer,
	}, nil
}

func (s *TermSearcher) Count() uint64 {
	return s.reader.Count()
}

func (s *TermSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *TermSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *TermSearcher) Next() (*DocumentMatch, error) {
	termMatch, err := s.reader.Next()
	if err != nil {
		return nil, err
	}

	if termMatch == nil {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(termMatch)
	// return doc match
	return docMatch, nil

}

func (s *TermSearcher) Advance(ID string) (*DocumentMatch, error) {
	termMatch, err := s.reader.Advance(ID)
	if err != nil {
		return nil, err
	}

	if termMatch == nil {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(termMatch)

	// return doc match
	return docMatch, nil
}

func (s *TermSearcher) Close() {
	s.reader.Close()
}
