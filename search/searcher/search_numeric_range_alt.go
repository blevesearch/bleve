//  Copyright (c) 2021 Couchbase, Inc.
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
	"fmt"
	"math"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	index "github.com/blevesearch/bleve_index_api"
)

type NumericRangeSearcherAlt struct {
	indexReader index.IndexReader
	reader      index.TermFieldReader
	scorer      *scorer.TermQueryScorer
	tfd         index.TermFieldDoc
}

func NewNumericRangeSearcherAlt(indexReader index.IndexReader,
	min *float64, max *float64, inclusiveMin, inclusiveMax *bool, field string,
	boost float64, options search.SearcherOptions) (search.Searcher, error) {
	// account for unbounded edges
	if min == nil {
		negInf := math.Inf(-1)
		min = &negInf
	}
	if max == nil {
		Inf := math.Inf(1)
		max = &Inf
	}
	if inclusiveMin == nil {
		defaultInclusiveMin := true
		inclusiveMin = &defaultInclusiveMin
	}
	if inclusiveMax == nil {
		defaultInclusiveMax := false
		inclusiveMax = &defaultInclusiveMax
	}

	if irnr, ok := indexReader.(index.IndexReaderNumericRange); ok {
		nr, err := irnr.NumericRangeReader(field, *min, *max, *inclusiveMax, *inclusiveMax)
		if err != nil {
			return nil, fmt.Errorf("error building numeric range reader: %v", err)
		}

		count, err := indexReader.DocCount()
		if err != nil {
			return nil, err
		}
		scorer := scorer.NewTermQueryScorer(fakeTermFromRange(*min, *max, *inclusiveMin, *inclusiveMax),
			field, boost, count, nr.Count(), options)

		return &NumericRangeSearcherAlt{
			indexReader: indexReader,
			reader:      nr,
			scorer:      scorer,
		}, nil
	}

	fmt.Println("oops unsupported")

	return NewMatchNoneSearcher(indexReader)
}

func (s *NumericRangeSearcherAlt) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
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

func (s *NumericRangeSearcherAlt) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
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

func (s *NumericRangeSearcherAlt) Close() error {
	return s.reader.Close()
}

func (s *NumericRangeSearcherAlt) Weight() float64 {
	return s.scorer.Weight()
}

func (s *NumericRangeSearcherAlt) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *NumericRangeSearcherAlt) Count() uint64 {
	return s.reader.Count()
}

func (s *NumericRangeSearcherAlt) Min() int {
	return 0
}

func (s *NumericRangeSearcherAlt) Size() int {
	return 0
}

func (s *NumericRangeSearcherAlt) DocumentMatchPoolSize() int {
	return 1
}

func fakeTermFromRange(min, max float64, inclusiveMin, inclusiveMax bool) []byte {
	start := "["
	if !inclusiveMin {
		start = "("
	}
	end := "]"
	if !inclusiveMax {
		end = ")"
	}
	return []byte(fmt.Sprintf("%s%f,%f%s", start, min, max, end))
}

// FIXME could also implement optimizable since we're still faking it with term field readers
