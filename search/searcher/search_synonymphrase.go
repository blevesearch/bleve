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
	var cs SynonymPhraseSearcher
	reflectStaticSizeSynonymSearcher = int(reflect.TypeOf(cs).Size())
}

type SearchAtPosition struct {
	Searcher search.Searcher
	FirstPos int
	LastPos  int
}

type SynonymPhraseSearcher struct {
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

func NewSynonymPhraseSearcher(ctx context.Context, indexReader index.IndexReader,
	qsearchers []SearchAtPosition, options search.SearcherOptions) (
	search.Searcher, error) {

	rv := SynonymPhraseSearcher{
		indexReader:           indexReader,
		options:               options,
		searchersWithPosition: qsearchers,
		currs:                 make([]*search.DocumentMatch, len(qsearchers)),
		scorer:                scorer.NewConjunctionQueryScorer(options),
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *SynonymPhraseSearcher) Size() int {
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

func (s *SynonymPhraseSearcher) computeQueryNorm() {
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

func (s *SynonymPhraseSearcher) initSearchers(ctx *search.SearchContext) error {
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

func (s *SynonymPhraseSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchersWithPosition {
		rv += searcher.Searcher.Weight()
	}
	return rv
}

func (s *SynonymPhraseSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchersWithPosition {
		searcher.Searcher.SetQueryNorm(qnorm)
	}
}

func (s *SynonymPhraseSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
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
		var FTLshit [][][]uint64
		for _, l := range s.currs {
			FTLshit = append(FTLshit, l.FTLSynonym)
		}
		for i := 0; i < len(FTLshit[0]); i++ {
			lastPos := FTLshit[0][i][len(FTLshit[0][i])-1]
			if theDFS(s.searchersWithPosition, 1, FTLshit, lastPos) {
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

func theDFS(origStream []SearchAtPosition, currIndex int, FTLshit [][][]uint64, lastPos uint64) bool {
	if currIndex == len(origStream) {
		return true
	}
	for _, FTL := range FTLshit[currIndex] {
		if FTL[0]-uint64(lastPos) == (uint64(origStream[currIndex].FirstPos - origStream[currIndex-1].LastPos)) {
			return theDFS(origStream, currIndex+1, FTLshit, FTL[len(FTL)-1])
		}
	}
	return false
}

func (s *SynonymPhraseSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
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

func (s *SynonymPhraseSearcher) advanceChild(ctx *search.SearchContext, i int, ID index.IndexInternalID) (err error) {
	if s.currs[i] != nil {
		ctx.DocumentMatchPool.Put(s.currs[i])
	}
	s.currs[i], err = s.searchersWithPosition[i].Searcher.Advance(ctx, ID)
	return err
}

func (s *SynonymPhraseSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchersWithPosition {
		sum += searcher.Searcher.Count()
	}
	return sum
}

func (s *SynonymPhraseSearcher) Close() (rv error) {
	for _, searcher := range s.searchersWithPosition {
		err := searcher.Searcher.Close()
		if err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *SynonymPhraseSearcher) Min() int {
	return 0
}

func (s *SynonymPhraseSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchersWithPosition {
		rv += s.Searcher.DocumentMatchPoolSize()
	}
	return rv
}
