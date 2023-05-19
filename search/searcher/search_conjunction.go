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
	"math"
	"reflect"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeConjunctionSearcher int

func init() {
	var cs ConjunctionSearcher
	reflectStaticSizeConjunctionSearcher = int(reflect.TypeOf(cs).Size())
}

type ConjunctionSearcher struct {
	indexReader       index.IndexReader
	searchers         OrderedSearcherList
	queryNorm         float64
	currs             []*search.DocumentMatch
	maxIDIdx          int
	scorer            *scorer.ConjunctionQueryScorer
	initialized       bool
	options           search.SearcherOptions
	bytesRead         uint64
	searcherPositions []*SearcherPosition
}

func NewConjunctionSearcher(ctx context.Context, indexReader index.IndexReader,
	qsearchers []search.Searcher, options search.SearcherOptions) (
	search.Searcher, error) {

	if ctx.Value(ctxKey("searcherPositions")) != nil {
		searcherPositions := ctx.Value(ctxKey("searcherPositions")).([]*SearcherPosition)
		rv := ConjunctionSearcher{
			indexReader:       indexReader,
			options:           options,
			searchers:         qsearchers,
			currs:             make([]*search.DocumentMatch, len(qsearchers)),
			scorer:            scorer.NewConjunctionQueryScorer(options),
			searcherPositions: searcherPositions,
		}
		rv.computeQueryNorm()
		return &rv, nil
	}

	// build the sorted downstream searchers
	searchers := make(OrderedSearcherList, len(qsearchers))
	for i, searcher := range qsearchers {
		searchers[i] = searcher
	}
	sort.Sort(searchers)
	// attempt the "unadorned" conjunction optimization only when we
	// do not need extra information like freq-norm's or term vectors
	if len(searchers) > 1 &&
		options.Score == "none" && !options.IncludeTermVectors {
		rv, err := optimizeCompositeSearcher(ctx, "conjunction:unadorned",
			indexReader, searchers, options)
		if err != nil || rv != nil {
			return rv, err
		}
	}

	// build our searcher
	rv := ConjunctionSearcher{
		indexReader: indexReader,
		options:     options,
		searchers:   searchers,
		currs:       make([]*search.DocumentMatch, len(searchers)),
		scorer:      scorer.NewConjunctionQueryScorer(options),
	}
	rv.computeQueryNorm()

	// attempt push-down conjunction optimization when there's >1 searchers
	if len(searchers) > 1 {
		rv, err := optimizeCompositeSearcher(ctx, "conjunction",
			indexReader, searchers, options)
		if err != nil || rv != nil {
			return rv, err
		}
	}

	return &rv, nil
}

func (s *ConjunctionSearcher) Size() int {
	sizeInBytes := reflectStaticSizeConjunctionSearcher + size.SizeOfPtr +
		s.scorer.Size()

	for _, entry := range s.searchers {
		sizeInBytes += entry.Size()
	}

	for _, entry := range s.currs {
		if entry != nil {
			sizeInBytes += entry.Size()
		}
	}

	return sizeInBytes
}

func (s *ConjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *ConjunctionSearcher) initSearchers(ctx *search.SearchContext) error {
	var err error
	// get all searchers pointing at their first match
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Next(ctx)
		if err != nil {
			return err
		}
	}
	s.initialized = true
	return nil
}

func (s *ConjunctionSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchers {
		rv += searcher.Weight()
	}
	return rv
}

func (s *ConjunctionSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *ConjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var rv *search.DocumentMatch
	var err error
OUTER:
	for s.maxIDIdx < len(s.currs) && s.currs[s.maxIDIdx] != nil {
		maxID := s.currs[s.maxIDIdx].IndexInternalID

		i := 0
		for i < len(s.currs) {
			if s.currs[i] == nil {
				return nil, nil
			}

			if i == s.maxIDIdx {
				i++
				continue
			}

			cmp := maxID.Compare(s.currs[i].IndexInternalID)
			if cmp == 0 {
				i++
				continue
			}

			if cmp < 0 {
				// maxID < currs[i], so we found a new maxIDIdx
				s.maxIDIdx = i

				// advance the positions where [0 <= x < i], since we
				// know they were equal to the former max entry
				maxID = s.currs[s.maxIDIdx].IndexInternalID
				for x := 0; x < i; x++ {
					err = s.advanceChild(ctx, x, maxID)
					if err != nil {
						return nil, err
					}
				}

				continue OUTER
			}

			// maxID > currs[i], so need to advance searchers[i]
			err = s.advanceChild(ctx, i, maxID)
			if err != nil {
				return nil, err
			}

			// don't bump i, so that we'll examine the just-advanced
			// currs[i] again
		}
		// if we get here, a doc matched all readers
		if s.searcherPositions == nil {
			rv = s.scorer.Score(ctx, s.currs)
		} else {
			// we only score the document if the relative positioning rule is satisfied
			// if not, we advance the maxIDIdx and try again
			var documentHitPositions = make([][][]uint64, len(s.currs))
			for i, match := range s.currs {
				documentHitPositions[i] = match.HitPositions
			}
			for i := 0; i < len(documentHitPositions[0]); i++ {
				startHit := documentHitPositions[0][i]
				if positionsAreCorrect(s.searcherPositions, 1, documentHitPositions, startHit[len(startHit)-1]) {
					rv = s.scorer.Score(ctx, s.currs)
					break
				}
			}
		}
		for i, searcher := range s.searchers {
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = searcher.Next(ctx)
			if err != nil {
				return nil, err
			}
		}
		if rv != nil {
			break
		}
	}
	return rv, nil
}

// positionsAreCorrect checks if the positions of the sub searchers hits are correct by performing a DFS on the documentHitPositions
// matrix that is passed.
// For each element in the current row of the matrix, it checks if the position of the first element minus the position of the last element
// in the previous row is equal to the difference between the first position of the current searcher and the last position of the previous searcher.
// If a row is reached where this is not true, the function returns false. If the end of the matrix is reached, the function returns true.
// The function is called recursively, starting at the second row.
func positionsAreCorrect(origStream []*SearcherPosition, currentRow int, documentHitPositions [][][]uint64, lastPos uint64) bool {
	if currentRow == len(origStream) {
		return true
	}
	for _, documentHitPosition := range documentHitPositions[currentRow] {
		documentRelativePosition := documentHitPosition[0] - lastPos
		expectedRelativePosition := origStream[currentRow].FirstPos - origStream[currentRow-1].LastPos
		if documentRelativePosition == expectedRelativePosition {
			newLastPos := documentHitPosition[len(documentHitPosition)-1]
			return positionsAreCorrect(origStream, currentRow+1, documentHitPositions, newLastPos)
		}
	}
	return false
}

func (s *ConjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	for i := range s.searchers {
		if s.currs[i] != nil && s.currs[i].IndexInternalID.Compare(ID) >= 0 {
			continue
		}
		err := s.advanceChild(ctx, i, ID)
		if err != nil {
			return nil, err
		}
	}
	return s.Next(ctx)
}

func (s *ConjunctionSearcher) advanceChild(ctx *search.SearchContext, i int, ID index.IndexInternalID) (err error) {
	if s.currs[i] != nil {
		ctx.DocumentMatchPool.Put(s.currs[i])
	}
	s.currs[i], err = s.searchers[i].Advance(ctx, ID)
	return err
}

func (s *ConjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *ConjunctionSearcher) Close() (rv error) {
	for _, searcher := range s.searchers {
		err := searcher.Close()
		if err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *ConjunctionSearcher) Min() int {
	return 0
}

func (s *ConjunctionSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchers {
		rv += s.DocumentMatchPoolSize()
	}
	return rv
}
