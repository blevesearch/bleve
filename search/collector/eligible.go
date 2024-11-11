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

type eligibleStore struct {
	ids []index.IndexInternalID
}

func (s *eligibleStore) AddID(doc *search.DocumentMatch) *search.DocumentMatch {
	copyOfID := make([]byte, len(doc.IndexInternalID))
	copy(copyOfID, doc.IndexInternalID)
	s.ids = append(s.ids, copyOfID)
	return doc
}

type EligibleCollector struct {
	size    int
	total   uint64
	took    time.Duration
	results search.DocumentMatchCollection

	store *eligibleStore
}

func NewEligibleCollector(size int) *EligibleCollector {
	return newEligibleCollector(size)
}

func getEligibleCollectorStore() *eligibleStore {
	return &eligibleStore{
		ids: make([]index.IndexInternalID, 0),
	}
}

func newEligibleCollector(size int) *EligibleCollector {
	// No sort order & skip always 0 since this is only to filter eligible docs.
	ec := &EligibleCollector{size: size}
	ec.store = getEligibleCollectorStore()
	return ec
}

func makeEligibleDocumentMatchHandler(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
	if ec, ok := ctx.Collector.(*EligibleCollector); ok {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				return nil
			}

			// No elements removed from the store here.
			doc := ec.store.AddID(d)
			ctx.DocumentMatchPool.Put(doc)
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

	return nil
}

func (ec *EligibleCollector) Results() search.DocumentMatchCollection {
	return nil
}

func (ec *EligibleCollector) IDs() []index.IndexInternalID {
	return ec.store.ids
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
