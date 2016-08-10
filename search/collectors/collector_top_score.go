//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package collectors

import (
	"container/list"
	"time"

	"golang.org/x/net/context"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type TopScoreCollector struct {
	k             int
	skip          int
	results       *list.List
	took          time.Duration
	maxScore      float64
	minScore      float64
	total         uint64
	facetsBuilder *search.FacetsBuilder
	actualResults search.DocumentMatchCollection
}

func NewTopScorerCollector(k int) *TopScoreCollector {
	return &TopScoreCollector{
		k:       k,
		skip:    0,
		results: list.New(),
	}
}

func NewTopScorerSkipCollector(k, skip int) *TopScoreCollector {
	return &TopScoreCollector{
		k:       k,
		skip:    skip,
		results: list.New(),
	}
}

func (tksc *TopScoreCollector) Total() uint64 {
	return tksc.total
}

func (tksc *TopScoreCollector) MaxScore() float64 {
	return tksc.maxScore
}

func (tksc *TopScoreCollector) Took() time.Duration {
	return tksc.took
}

var COLLECT_CHECK_DONE_EVERY = uint64(1024)

func (tksc *TopScoreCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// search context with enough pre-allocated document matches
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(tksc.k + tksc.skip + searcher.DocumentMatchPoolSize()),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if tksc.total%COLLECT_CHECK_DONE_EVERY == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if tksc.facetsBuilder != nil {
			err = tksc.facetsBuilder.Update(next)
			if err != nil {
				break
			}
		}
		tksc.collectSingle(searchContext, next)

		next, err = searcher.Next(searchContext)
	}
	// finalize actual results
	tksc.actualResults, err = tksc.finalizeResults(reader)
	if err != nil {
		return err
	}

	// compute search duration
	tksc.took = time.Since(startTime)
	if err != nil {
		return err
	}
	return nil
}

func (tksc *TopScoreCollector) collectSingle(ctx *search.SearchContext, d *search.DocumentMatch) {
	// increment total hits
	tksc.total++

	// update max score
	if d.Score > tksc.maxScore {
		tksc.maxScore = d.Score
	}

	if d.Score <= tksc.minScore {
		ctx.DocumentMatchPool.Put(d)
		return
	}

	for e := tksc.results.Front(); e != nil; e = e.Next() {
		curr := e.Value.(*search.DocumentMatch)
		if d.Score <= curr.Score {

			tksc.results.InsertBefore(d, e)
			// if we just made the list too long
			if tksc.results.Len() > (tksc.k + tksc.skip) {
				// remove the head
				removed := tksc.results.Remove(tksc.results.Front()).(*search.DocumentMatch)
				tksc.minScore = removed.Score
				ctx.DocumentMatchPool.Put(removed)
			}
			return
		}
	}
	// if we got to the end, we still have to add it
	tksc.results.PushBack(d)
	if tksc.results.Len() > (tksc.k + tksc.skip) {
		// remove the head
		removed := tksc.results.Remove(tksc.results.Front()).(*search.DocumentMatch)
		tksc.minScore = removed.Score
		ctx.DocumentMatchPool.Put(removed)
	}
}

func (tksc *TopScoreCollector) Results() search.DocumentMatchCollection {
	return tksc.actualResults
}

func (tksc *TopScoreCollector) finalizeResults(r index.IndexReader) (search.DocumentMatchCollection, error) {
	if tksc.results.Len()-tksc.skip > 0 {
		rv := make(search.DocumentMatchCollection, tksc.results.Len()-tksc.skip)
		i := 0
		skipped := 0
		for e := tksc.results.Back(); e != nil; e = e.Prev() {
			if skipped < tksc.skip {
				skipped++
				continue
			}
			var err error
			rv[i] = e.Value.(*search.DocumentMatch)
			rv[i].ID, err = r.FinalizeDocID(rv[i].IndexInternalID)
			if err != nil {
				return nil, err
			}
			i++
		}
		return rv, nil
	}
	return search.DocumentMatchCollection{}, nil
}

func (tksc *TopScoreCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	tksc.facetsBuilder = facetsBuilder
}

func (tksc *TopScoreCollector) FacetResults() search.FacetResults {
	if tksc.facetsBuilder != nil {
		return tksc.facetsBuilder.Results()
	}
	return search.FacetResults{}
}
