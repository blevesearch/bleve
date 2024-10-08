//  Copyright (c) 2024 Couchbase, Inc.
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

package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type EligibleCollector struct {
	size    int
	total   uint64
	took    time.Duration
	results search.DocumentMatchCollection

	store collectorStore
}

func NewEligibleCollector(size int) *EligibleCollector {
	return newEligibleCollector(size)
}

func newEligibleCollector(size int) *EligibleCollector {
	// No sort order & skip always 0 since this is only to filter eligible docs.
	ec := &EligibleCollector{size: size}

	// comparator is a dummy here
	ec.store = getOptimalCollectorStore(size, 0, func(i, j *search.DocumentMatch) int {
		return 0
	})

	return ec
}

func makeEligibleDocumentMatchHandler(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
	if ec, ok := ctx.Collector.(*EligibleCollector); ok {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				return nil
			}

			// No elements removed from the store here.
			_ = ec.store.Add(d)
			return nil
		}, nil
	}

	return nil, fmt.Errorf("eligiblity collector not available")
}

func (ec *EligibleCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	backingSize := ec.size
	if backingSize > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(backingSize+searcher.DocumentMatchPoolSize(), 0),
		Collector:         ec,
		IndexReader:       reader,
	}

	dmHandler, err := makeEligibleDocumentMatchHandler(searchContext)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		search.RecordSearchCost(ctx, search.AbortM, 0)
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if ec.total%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		ec.total++

		err = dmHandler(next)
		if err != nil {
			break
		}

		next, err = searcher.Next(searchContext)
	}
	if err != nil {
		return err
	}

	// help finalize/flush the results in case
	// of custom document match handlers.
	err = dmHandler(nil)
	if err != nil {
		return err
	}

	// compute search duration
	ec.took = time.Since(startTime)

	// finalize actual results
	err = ec.finalizeResults(reader)
	if err != nil {
		return err
	}
	return nil
}

func (ec *EligibleCollector) finalizeResults(r index.IndexReader) error {
	var err error
	ec.results, err = ec.store.Final(0, func(doc *search.DocumentMatch) error {
		// Adding the results to the store without any modifications since we don't
		// require the external ID of the filtered hits.
		return nil
	})
	return err
}

func (ec *EligibleCollector) Results() search.DocumentMatchCollection {
	return ec.results
}

func (ec *EligibleCollector) Total() uint64 {
	return ec.total
}

// No concept of scoring in the eligible collector.
func (ec *EligibleCollector) MaxScore() float64 {
	return 0
}

func (ec *EligibleCollector) Took() time.Duration {
	return ec.took
}

func (ec *EligibleCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	// facet unsupported for pre-filtering in KNN search
}

func (ec *EligibleCollector) FacetResults() search.FacetResults {
	// facet unsupported for pre-filtering in KNN search
	return nil
}
