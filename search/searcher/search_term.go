//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"context"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeTermSearcher int

func init() {
	var ts TermSearcher
	reflectStaticSizeTermSearcher = int(reflect.TypeOf(ts).Size())
}

type TermSearcher struct {
	indexReader index.IndexReader
	reader      index.TermFieldReader
	scorer      *scorer.TermQueryScorer
	tfd         index.TermFieldDoc
}

func NewTermSearcher(ctx context.Context, indexReader index.IndexReader, term string, field string, boost float64, options search.SearcherOptions) (search.Searcher, error) {
	if isTermQuery(ctx) {
		ctx = context.WithValue(ctx, search.QueryTypeKey, search.Term)
	}
	return NewTermSearcherBytes(ctx, indexReader, []byte(term), field, boost, options)
}

func NewTermSearcherBytes(ctx context.Context, indexReader index.IndexReader, term []byte, field string, boost float64, options search.SearcherOptions) (search.Searcher, error) {
	needFreqNorm := options.Score != "none"
	if fieldTermSynonyms, ok := ctx.Value(search.FieldTermSynonymsKey).(search.FieldTermSynonyms); ok {
		if termSynonyms, ok := fieldTermSynonyms[field]; ok {
			synonyms := termSynonyms[string(term)]
			if len(synonyms) > 0 {
				return newSynonymSearcherFromReader(ctx, indexReader, term, synonyms, field, boost, options, needFreqNorm)
			}
		}
	}
	reader, err := indexReader.TermFieldReader(ctx, term, field, needFreqNorm, needFreqNorm, options.IncludeTermVectors)
	if err != nil {
		return nil, err
	}
	return newTermSearcherFromReader(indexReader, reader, term, field, boost, options)
}

func newSynonymSearcherFromReader(ctx context.Context, indexReader index.IndexReader, term []byte, synonyms []string,
	field string, boost float64, options search.SearcherOptions, needFreqNorm bool) (search.Searcher, error) {
	qsearchers := make([]search.Searcher, 0, len(synonyms)+1)
	qsearchersClose := func() {
		for _, searcher := range qsearchers {
			if searcher != nil {
				_ = searcher.Close()
			}
		}
	}
	for _, synonym := range synonyms {
		synonymReader, err := indexReader.TermFieldReader(ctx, []byte(synonym), field, needFreqNorm, needFreqNorm, options.IncludeTermVectors)
		if err != nil {
			return nil, err
		}
		searcher, err := newTermSearcherFromReader(indexReader, synonymReader, []byte(synonym), field, boost, options)
		if err != nil {
			qsearchersClose()
			return nil, err
		}
		qsearchers = append(qsearchers, searcher)
	}
	reader, err := indexReader.TermFieldReader(ctx, term, field, needFreqNorm, needFreqNorm, options.IncludeTermVectors)
	if err != nil {
		return nil, err
	}
	searcher, err := newTermSearcherFromReader(indexReader, reader, term, field, boost, options)
	if err != nil {
		qsearchersClose()
		return nil, err
	}
	qsearchers = append(qsearchers, searcher)
	rv, err := newDisjunctionSearcher(ctx, indexReader, qsearchers, 1, options, true)
	if err != nil {
		qsearchersClose()
		return nil, err
	}
	return rv, nil
}

func newTermSearcherFromReader(indexReader index.IndexReader, reader index.TermFieldReader,
	term []byte, field string, boost float64, options search.SearcherOptions) (*TermSearcher, error) {
	count, err := indexReader.DocCount()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	scorer := scorer.NewTermQueryScorer(term, field, boost, count, reader.Count(), options)
	return &TermSearcher{
		indexReader: indexReader,
		reader:      reader,
		scorer:      scorer,
	}, nil
}

func (s *TermSearcher) Size() int {
	return reflectStaticSizeTermSearcher + size.SizeOfPtr +
		s.reader.Size() +
		s.tfd.Size() +
		s.scorer.Size()
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
	termMatch, err := s.reader.Next(s.tfd.Reset())
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

func (s *TermSearcher) Optimize(kind string, octx index.OptimizableContext) (
	index.OptimizableContext, error) {
	o, ok := s.reader.(index.Optimizable)
	if ok {
		return o.Optimize(kind, octx)
	}

	return nil, nil
}

func isTermQuery(ctx context.Context) bool {
	if ctx != nil {
		// if the ctx already has a value set for query type
		// it would've been done at a non term searcher level.
		_, ok := ctx.Value(search.QueryTypeKey).(string)
		return !ok
	}
	// if the context is nil, then don't set the query type
	return false
}
