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

// TODO run inverted query or not based on the cardinality check
// make this user configurable?
// make this cardinality check as light as possible!

package collector

import (
	"context"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type FilterCardinalityCollector struct {
	size  int
	total uint64
	took  time.Duration
}

func NewFilterCardinalityCollector(size int) *FilterCardinalityCollector {
	return newFilterCardinalityCollector(size)
}

func newFilterCardinalityCollector(size int) *FilterCardinalityCollector {
	// No sort order & skip always 0 since this is only to filter eligible docs.
	ec := &FilterCardinalityCollector{size: size}

	return ec
}

func (ec *FilterCardinalityCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
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

		next, err = searcher.Next(searchContext)
	}
	if err != nil {
		return err
	}

	// compute search duration
	ec.took = time.Since(startTime)
	return nil
}

func (ec *FilterCardinalityCollector) Results() search.DocumentMatchCollection {
	return nil
}

func (ec *FilterCardinalityCollector) Total() uint64 {
	return ec.total
}

// No concept of scoring in the eligible collector.
func (ec *FilterCardinalityCollector) MaxScore() float64 {
	return 0
}

func (ec *FilterCardinalityCollector) Took() time.Duration {
	return ec.took
}

func (ec *FilterCardinalityCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	// facet unsupported for pre-filtering in KNN search
}

func (ec *FilterCardinalityCollector) FacetResults() search.FacetResults {
	// facet unsupported for pre-filtering in KNN search
	return nil
}
