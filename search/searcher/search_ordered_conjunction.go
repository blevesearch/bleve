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

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeSynonymSearcher int

func init() {
	var cs OrderedConjunctionSearcher
	reflectStaticSizeSynonymSearcher = int(reflect.TypeOf(cs).Size())
}

// SearchAtPosition is a struct that contains a searcher and the first and last position of the searcher in the query.
//   - Searcher is the main searcher for the token.
//   - FirstPos is the first position of the searcher.
//   - LastPos is the last position of the searcher.
type SearchAtPosition struct {
	Searcher search.Searcher
	FirstPos uint64
	LastPos  uint64
}

// OrderedConjunctionSearcher implements a specialized conjunction searcher.
// In addition to ensuring that all sub-searchers match a document, it also ensures that
// the sub-searchers match in the correct order in the document.
// The input sub-searchers must be in order, and must all have the same field.
// It uses two properties, first and last position, for each sub searcher,
// which ensures that the relative positioning of each sub searchers hits in the document is maintained.
//
// For example - if there are 3 sub-searchers with the following parameters
//   - searcher1: first position = 4, last position = 4
//   - searcher2: first position = 6, last position = 8
//   - searcher3: first position = 13, last position = 16
//
// Then any document hit must have
//   - searcher1 hit (extending positions [X,X])
//   - searcher2 hit 2 positions after searcher1 hit (extending positions [X+2,X+4])
//   - searcher3 hit 5 positions after searcher2 hit (extending positions [X+9,X+12])
//
// thus for each sub searcher:
//   - the hit in the document must be at position equal to its first position - the previous searcher's last position
//   - the hit for the first sub searcher in the sequence can be anywhere in the document.

// searchersWithPosition is a slice of searchAtPosition, which is the input to the searcher.
type OrderedConjunctionSearcher struct {
	indexReader           index.IndexReader
	searchersWithPosition []SearchAtPosition
	queryNorm             float64
	currs                 []*search.DocumentMatch
	maxIDIdx              int
	scorer                *scorer.ConjunctionQueryScorer
	initialized           bool
	options               search.SearcherOptions
	bytesRead             uint64
}

func NewOrderedConjunctionSearcher(ctx context.Context, indexReader index.IndexReader,
	qsearchers []SearchAtPosition, options search.SearcherOptions) (
	search.Searcher, error) {

	rv := OrderedConjunctionSearcher{
		indexReader:           indexReader,
		options:               options,
		searchersWithPosition: qsearchers,
		currs:                 make([]*search.DocumentMatch, len(qsearchers)),
		scorer:                scorer.NewConjunctionQueryScorer(options),
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *OrderedConjunctionSearcher) Size() int {
	sizeInBytes := reflectStaticSizeConjunctionSearcher + size.SizeOfPtr +
		s.scorer.Size()

	for _, entry := range s.searchersWithPosition {
		sizeInBytes += entry.Searcher.Size()
	}

	for _, entry := range s.currs {
		if entry != nil {
			sizeInBytes += entry.Size()
		}
	}

	return sizeInBytes
}

func (s *OrderedConjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchersWithPosition {
		sumOfSquaredWeights += searcher.Searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchersWithPosition {
		searcher.Searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *OrderedConjunctionSearcher) initSearchers(ctx *search.SearchContext) error {
	var err error
	// get all searchers pointing at their first match
	for i, searcher := range s.searchersWithPosition {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Searcher.Next(ctx)
		if err != nil {
			return err
		}
	}
	s.initialized = true
	return nil
}

func (s *OrderedConjunctionSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchersWithPosition {
		rv += searcher.Searcher.Weight()
	}
	return rv
}

func (s *OrderedConjunctionSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchersWithPosition {
		searcher.Searcher.SetQueryNorm(qnorm)
	}
}

func (s *OrderedConjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var rv *search.DocumentMatch
	var err error
	var found bool
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
		// we only score the document if the relative positioning rule is satisfied
		// if not, we advance the maxIDIdx and try again
		var documentHitPositions = make([][][]uint64, len(s.currs))
		for i, match := range s.currs {
			documentHitPositions[i] = match.HitPositions
		}
		for i := 0; i < len(documentHitPositions[0]); i++ {
			startHit := documentHitPositions[0][i]
			if positionsAreCorrect(s.searchersWithPosition, 1, documentHitPositions, startHit[len(startHit)-1]) {
				rv = s.scorer.Score(ctx, s.currs)
				found = true
				break
			}
		}
		for i, searcher := range s.searchersWithPosition {
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = searcher.Searcher.Next(ctx)
			if err != nil {
				return nil, err
			}
		}
		if found {
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
func positionsAreCorrect(origStream []SearchAtPosition, currentRow int, documentHitPositions [][][]uint64, lastPos uint64) bool {
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

func (s *OrderedConjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	for i := range s.searchersWithPosition {
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

func (s *OrderedConjunctionSearcher) advanceChild(ctx *search.SearchContext, i int, ID index.IndexInternalID) (err error) {
	if s.currs[i] != nil {
		ctx.DocumentMatchPool.Put(s.currs[i])
	}
	s.currs[i], err = s.searchersWithPosition[i].Searcher.Advance(ctx, ID)
	return err
}

func (s *OrderedConjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchersWithPosition {
		sum += searcher.Searcher.Count()
	}
	return sum
}

func (s *OrderedConjunctionSearcher) Close() (rv error) {
	for _, searcher := range s.searchersWithPosition {
		err := searcher.Searcher.Close()
		if err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *OrderedConjunctionSearcher) Min() int {
	return 0
}

func (s *OrderedConjunctionSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchersWithPosition {
		rv += s.Searcher.DocumentMatchPoolSize()
	}
	return rv
}
