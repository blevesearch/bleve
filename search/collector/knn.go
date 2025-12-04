//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package collector

import (
	"context"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type collectStoreKNN struct {
	internalHeaps []collectorStore
	kValues       []int64
	allHits       map[*search.DocumentMatch]struct{}
	ejectedDocs   map[*search.DocumentMatch]struct{}
}

func newStoreKNN(internalHeaps []collectorStore, kValues []int64) *collectStoreKNN {
	return &collectStoreKNN{
		internalHeaps: internalHeaps,
		kValues:       kValues,
		ejectedDocs:   make(map[*search.DocumentMatch]struct{}),
		allHits:       make(map[*search.DocumentMatch]struct{}),
	}
}

// Adds a document to the collector store and returns the documents that were ejected
// from the store. The documents that were ejected from the store are the ones that
// were not in the top K documents for any of the heaps.
// These document are put back into the pool document match pool in the KNN Collector.
func (c *collectStoreKNN) AddDocument(doc *search.DocumentMatch) []*search.DocumentMatch {
	for heapIdx := 0; heapIdx < len(c.internalHeaps); heapIdx++ {
		if _, ok := doc.ScoreBreakdown[heapIdx]; !ok {
			continue
		}
		ejectedDoc := c.internalHeaps[heapIdx].AddNotExceedingSize(doc, int(c.kValues[heapIdx]))
		if ejectedDoc != nil {
			delete(ejectedDoc.ScoreBreakdown, heapIdx)
			c.ejectedDocs[ejectedDoc] = struct{}{}
		}
	}
	var rv []*search.DocumentMatch
	for doc := range c.ejectedDocs {
		if len(doc.ScoreBreakdown) == 0 {
			rv = append(rv, doc)
		}
		// clear out the ejectedDocs map to reuse it in the next AddDocument call
		delete(c.ejectedDocs, doc)
	}
	return rv
}

func (c *collectStoreKNN) Final(fixup collectorFixup) (search.DocumentMatchCollection, error) {
	for _, heap := range c.internalHeaps {
		for _, doc := range heap.Internal() {
			// duplicates may be present across the internal heaps
			// meaning the same document match may be in the top K
			// for multiple KNN queries.
			c.allHits[doc] = struct{}{}
		}
	}
	size := len(c.allHits)
	if size <= 0 {
		return make(search.DocumentMatchCollection, 0), nil
	}
	rv := make(search.DocumentMatchCollection, size)
	i := 0
	for doc := range c.allHits {
		if fixup != nil {
			err := fixup(doc)
			if err != nil {
				return nil, err
			}
		}
		rv[i] = doc
		i++
	}
	return rv, nil
}

// -----------------------------------------------------------------------------------------
// knnMerger is a preprocessing layer on top of collectStoreKNN that merges
// duplicate document matches before adding them to the underlying KNN store.
// Since KNN searchers may return the same document multiple times (with different
// scoreBreakdown values for each KNN query), we must merge these to retain the
// best score per KNN query before adding them to the KNN store.
// NOTE: This implementation assumes documents arrive in increasing order of their
// internal IDs, which is guaranteed by all searchers in bleve.
type knnMerger struct {
	// curr holds the current document match being accumulated during the merge process.
	curr *search.DocumentMatch
}

func newKNNMerger() *knnMerger {
	return &knnMerger{}
}

// Merge merges duplicate document matches by combining their score breakdowns.
// Returns nil if the incoming doc was merged into the current document.
// Returns the completed previous document when a new document with a different ID arrives.
// The returned DocumentMatch is ready for further processing.
func (c *knnMerger) Merge(ctx *search.SearchContext, doc *search.DocumentMatch) (*search.DocumentMatch, error) {
	// see if the document has been seen before
	if c.curr != nil && c.curr.IndexInternalID.Equals(doc.IndexInternalID) {
		// merge the score breakdowns
		c.curr.ScoreBreakdown, c.curr.Expl = search.MergeScoreExplBreakdown(
			c.curr.ScoreBreakdown, doc.ScoreBreakdown,
			c.curr.Expl, doc.Expl)
		// recycle the incoming document now that it's merged
		ctx.DocumentMatchPool.Put(doc)
		// return nil since no document to process further
		return nil, nil
	}
	// now we are sure that this is a new document, check if we have an existing
	// document to return for processing
	if c.curr != nil {
		// we have an existing document, return it for processing
		toReturn := c.curr
		// set the current to the incoming document
		c.curr = doc
		return toReturn, nil
	}
	// first document seen, set it as current and return nil
	c.curr = doc
	return nil, nil
}

// Current returns the current document match being merged, if any.
// Call this after processing all documents to flush the last document.
func (c *knnMerger) Current() *search.DocumentMatch {
	return c.curr
}

// -----------------------------------------------------------------------------------------

func MakeKNNDocMatchHandler(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
	var hc *KNNCollector
	var ok bool
	if hc, ok = ctx.Collector.(*KNNCollector); ok {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				return nil
			}
			toRelease := hc.knnStore.AddDocument(d)
			for _, doc := range toRelease {
				ctx.DocumentMatchPool.Put(doc)
			}
			return nil
		}, nil
	}
	return nil, nil
}

func GetNewKNNCollectorStore(kArray []int64) *collectStoreKNN {
	internalHeaps := make([]collectorStore, len(kArray))
	for knnIdx, k := range kArray {
		// TODO - Check if the datatype of k can be made into an int instead of int64
		idx := knnIdx
		internalHeaps[idx] = getOptimalCollectorStore(int(k), 0, func(i, j *search.DocumentMatch) int {
			if i.ScoreBreakdown[idx] < j.ScoreBreakdown[idx] {
				return 1
			}
			return -1
		})
	}
	return newStoreKNN(internalHeaps, kArray)
}

// implements Collector interface
type KNNCollector struct {
	// merger merges duplicate document matches before adding to knnStore
	merger *knnMerger
	// knnStore is the underlying store for KNN document matches
	knnStore *collectStoreKNN
	size     int
	total    uint64
	took     time.Duration
	results  search.DocumentMatchCollection
	maxScore float64
}

func NewKNNCollector(kArray []int64, size int64) *KNNCollector {
	return &KNNCollector{
		merger:   newKNNMerger(),
		knnStore: GetNewKNNCollectorStore(kArray),
		size:     int(size),
	}
}

func (hc *KNNCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// pre-allocate enough space in the DocumentMatchPool
	// unless the sum of K is too large, then cap it
	// everything should still work, just allocates DocumentMatches on demand
	backingSize := hc.size
	if backingSize > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(backingSize+searcher.DocumentMatchPoolSize(), 0),
		Collector:         hc,
		IndexReader:       reader,
	}

	dmHandlerMakerKNN := MakeKNNDocMatchHandler
	if cv := ctx.Value(search.MakeKNNDocumentMatchHandlerKey); cv != nil {
		dmHandlerMakerKNN = cv.(search.MakeKNNDocumentMatchHandler)
	}
	// use the application given builder for making the custom document match
	// handler and perform callbacks/invocations on the newly made handler.
	dmHandler, err := dmHandlerMakerKNN(searchContext)
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
		if hc.total%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		hc.total++

		// since we may get duplicate document matches from the KNN searcher,
		// we must merge them before adding to the KNN store, keeping the
		// best scoreBreakdown for each KNN query per document.
		next, err = hc.merger.Merge(searchContext, next)
		if err != nil {
			break
		}

		// we may have stored next for merging, or we may have completed a merge
		// and have a document ready for further processing, so next can be nil
		if next != nil {
			err = dmHandler(next)
			if err != nil {
				break
			}
		}

		next, err = searcher.Next(searchContext)
	}
	if err != nil {
		return err
	}

	// flush the last merged document if any
	if lastDoc := hc.merger.Current(); lastDoc != nil {
		err = dmHandler(lastDoc)
		if err != nil {
			return err
		}
	}

	// help finalize/flush the results in case
	// of custom document match handlers.
	err = dmHandler(nil)
	if err != nil {
		return err
	}

	// compute search duration
	hc.took = time.Since(startTime)

	// finalize actual results
	err = hc.finalizeResults(reader)
	if err != nil {
		return err
	}
	return nil
}

func (hc *KNNCollector) finalizeResults(r index.IndexReader) error {
	var err error
	hc.results, err = hc.knnStore.Final(func(doc *search.DocumentMatch) error {
		if doc.ID == "" {
			// look up the id since we need it for lookup
			var err error
			doc.ID, err = r.ExternalID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (hc *KNNCollector) Results() search.DocumentMatchCollection {
	return hc.results
}

func (hc *KNNCollector) Total() uint64 {
	return hc.total
}

func (hc *KNNCollector) MaxScore() float64 {
	return hc.maxScore
}

func (hc *KNNCollector) Took() time.Duration {
	return hc.took
}

func (hc *KNNCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	// facet unsupported for vector search
}

func (hc *KNNCollector) FacetResults() search.FacetResults {
	// facet unsupported for vector search
	return nil
}
