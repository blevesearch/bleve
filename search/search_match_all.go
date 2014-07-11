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

type MatchAllSearcher struct {
	index  index.Index
	query  *MatchAllQuery
	reader index.DocIdReader
	scorer *ConstantScorer
}

func NewMatchAllSearcher(index index.Index, query *MatchAllQuery) (*MatchAllSearcher, error) {
	reader, err := index.DocIdReader("", "")
	if err != nil {
		return nil, err
	}
	scorer := NewConstantScorer(query, 1.0, query.Explain)
	return &MatchAllSearcher{
		index:  index,
		query:  query,
		reader: reader,
		scorer: scorer,
	}, nil
}

func (s *MatchAllSearcher) Count() uint64 {
	return s.index.DocCount()
}

func (s *MatchAllSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *MatchAllSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *MatchAllSearcher) Next() (*DocumentMatch, error) {
	id, err := s.reader.Next()
	if err != nil {
		return nil, err
	}

	if id == "" {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(id)
	// return doc match
	return docMatch, nil

}

func (s *MatchAllSearcher) Advance(ID string) (*DocumentMatch, error) {
	id, err := s.reader.Advance(ID)
	if err != nil {
		return nil, err
	}

	if id == "" {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(id)

	// return doc match
	return docMatch, nil
}

func (s *MatchAllSearcher) Close() {
	s.reader.Close()
}
