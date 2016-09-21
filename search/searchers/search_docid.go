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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

// DocIDSearcher returns documents matching a predefined set of identifiers.
type DocIDSearcher struct {
	reader index.DocIDReader
	scorer *scorers.ConstantScorer
	count  int
}

func NewDocIDSearcher(indexReader index.IndexReader, ids []string, boost float64,
	explain bool) (searcher *DocIDSearcher, err error) {

	reader, err := indexReader.DocIDReaderOnly(ids)
	if err != nil {
		return nil, err
	}
	scorer := scorers.NewConstantScorer(1.0, boost, explain)
	return &DocIDSearcher{
		scorer: scorer,
		reader: reader,
		count:  len(ids),
	}, nil
}

func (s *DocIDSearcher) Count() uint64 {
	return uint64(s.count)
}

func (s *DocIDSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *DocIDSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *DocIDSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	ctx.LowScoreFilter = 0

	docidMatch, err := s.reader.Next()
	if err != nil {
		return nil, err
	}
	if docidMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(ctx, docidMatch)
	return docMatch, nil
}

func (s *DocIDSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	docidMatch, err := s.reader.Advance(ID)
	if err != nil {
		return nil, err
	}
	if docidMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(ctx, docidMatch)
	return docMatch, nil
}

func (s *DocIDSearcher) Close() error {
	return nil
}

func (s *DocIDSearcher) Min() int {
	return 0
}

func (s *DocIDSearcher) DocumentMatchPoolSize() int {
	return 1
}
