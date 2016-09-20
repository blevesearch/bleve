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
	"github.com/blevesearch/bleve/search/scorers"
)

type TermSearcher struct {
	indexReader index.IndexReader
	term        string
	field       string
	reader      index.TermFieldReader
	scorer      *scorers.TermQueryScorer
	tfd         index.TermFieldDoc
	ctx         *search.SearchContext
	filter      index.FreqNormFilter
	explain     bool
}

func NewTermSearcher(indexReader index.IndexReader, term string, field string, boost float64, explain bool) (*TermSearcher, error) {
	reader, err := indexReader.TermFieldReader([]byte(term), field, true, true, true)
	if err != nil {
		return nil, err
	}
	count, err := indexReader.DocCount()
	if err != nil {
		return nil, err
	}
	scorer := scorers.NewTermQueryScorer(term, field, boost, count, reader.Count(), explain)

	s := &TermSearcher{
		indexReader: indexReader,
		term:        term,
		field:       field,
		explain:     explain,
		reader:      reader,
		scorer:      scorer,
	}

	s.filter = func(freq uint64, norm float64) bool {
		_, score := scorer.ScoreFreqNorm(freq, norm)
		if score < s.ctx.LowScoreFilter {
			s.ctx.LowScoreNumMatches++
			return true
		}
		return false
	}

	return s, nil
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

func (s *TermSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	var filter index.FreqNormFilter

	if ctx.LowScoreFilter > 0 {
		filter = s.filter
		s.ctx = ctx
	}

	termMatch, err := s.reader.Next(s.tfd.Reset(), filter)
	if err != nil {
		return nil, err
	}

	if termMatch == nil {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(ctx, termMatch)
	// return doc match
	return docMatch, nil

}

func (s *TermSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	termMatch, err := s.reader.Advance(ID, s.tfd.Reset())
	if err != nil {
		return nil, err
	}

	if termMatch == nil {
		return nil, nil
	}

	// score match
	docMatch := s.scorer.Score(ctx, termMatch)

	// return doc match
	return docMatch, nil
}

func (s *TermSearcher) Close() error {
	return s.reader.Close()
}

func (s *TermSearcher) Min() int {
	return 0
}

func (s *TermSearcher) DocumentMatchPoolSize() int {
	return 1
}
