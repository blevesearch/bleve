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

	"github.com/blevesearch/bleve/search"
)

type TopScoreCollector struct {
	k             int
	skip          int
	results       *list.List
	took          time.Duration
	maxScore      float64
	total         uint64
	facetsBuilder *search.FacetsBuilder
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

func (tksc *TopScoreCollector) Collect(ctx context.Context, searcher search.Searcher) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next()
	}
	for err == nil && next != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			tksc.collectSingle(next)
			if tksc.facetsBuilder != nil {
				err = tksc.facetsBuilder.Update(next)
				if err != nil {
					break
				}
			}
			next, err = searcher.Next()
		}
	}
	// compute search duration
	tksc.took = time.Since(startTime)
	if err != nil {
		return err
	}
	return nil
}

func (tksc *TopScoreCollector) collectSingle(dm *search.DocumentMatch) {
	// increment total hits
	tksc.total++

	// update max score
	if dm.Score > tksc.maxScore {
		tksc.maxScore = dm.Score
	}

	for e := tksc.results.Front(); e != nil; e = e.Next() {
		curr := e.Value.(*search.DocumentMatch)
		if dm.Score < curr.Score {

			tksc.results.InsertBefore(dm, e)
			// if we just made the list too long
			if tksc.results.Len() > (tksc.k + tksc.skip) {
				// remove the head
				tksc.results.Remove(tksc.results.Front())
			}
			return
		}
	}
	// if we got to the end, we still have to add it
	tksc.results.PushBack(dm)
	if tksc.results.Len() > (tksc.k + tksc.skip) {
		// remove the head
		tksc.results.Remove(tksc.results.Front())
	}
}

func (tksc *TopScoreCollector) Results() search.DocumentMatchCollection {
	if tksc.results.Len()-tksc.skip > 0 {
		rv := make(search.DocumentMatchCollection, tksc.results.Len()-tksc.skip)
		i := 0
		skipped := 0
		for e := tksc.results.Back(); e != nil; e = e.Prev() {
			if skipped < tksc.skip {
				skipped++
				continue
			}
			rv[i] = e.Value.(*search.DocumentMatch)
			i++
		}
		return rv
	}
	return search.DocumentMatchCollection{}
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
