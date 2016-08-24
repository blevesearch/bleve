//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
//
package collectors

import (
	"container/heap"
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"golang.org/x/net/context"
)

type HeapCollector struct {
	size          int
	skip          int
	total         uint64
	maxScore      float64
	took          time.Duration
	sort          search.SortOrder
	results       search.DocumentMatchCollection
	facetsBuilder *search.FacetsBuilder

	lowestMatchOutsideResults *search.DocumentMatch
}

var COLLECT_CHECK_DONE_EVERY = uint64(1024)

func NewHeapCollector(size int, skip int, sort search.SortOrder) *HeapCollector {
	hc := &HeapCollector{size: size, skip: skip, sort: sort}
	heap.Init(hc)
	return hc
}

func (hc *HeapCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// search context with enough pre-allocated document matches
	// we keep references to size+skip ourselves
	// plus possibly one extra for the highestMatchOutsideResults
	// plus the amount required by the searcher tree
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(hc.size + hc.skip + 1 + searcher.DocumentMatchPoolSize()),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if hc.total%COLLECT_CHECK_DONE_EVERY == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if hc.facetsBuilder != nil {
			err = hc.facetsBuilder.Update(next)
			if err != nil {
				break
			}
		}

		err = hc.collectSingle(searchContext, reader, next)
		if err != nil {
			break
		}

		next, err = searcher.Next(searchContext)
	}
	// compute search duration
	hc.took = time.Since(startTime)
	if err != nil {
		return err
	}
	// finalize actual results
	err = hc.finalizeResults(reader)
	if err != nil {
		return err
	}
	return nil
}

func (hc *HeapCollector) collectSingle(ctx *search.SearchContext, reader index.IndexReader, d *search.DocumentMatch) error {
	// increment total hits
	hc.total++
	d.HitNumber = hc.total

	// update max score
	if d.Score > hc.maxScore {
		hc.maxScore = d.Score
	}

	var err error
	// see if we need to load ID (at this early stage, for example to sort on it)
	if hc.sort.RequiresDocID() {
		d.ID, err = reader.FinalizeDocID(d.IndexInternalID)
		if err != nil {
			return err
		}
	}

	// see if we need to load the stored fields
	if len(hc.sort.RequiredFields()) > 0 {
		// find out which fields haven't been loaded yet
		fieldsToLoad := d.CachedFieldTerms.FieldsNotYetCached(hc.sort.RequiredFields())
		// look them up
		fieldTerms, err := reader.DocumentFieldTerms(d.IndexInternalID, fieldsToLoad)
		if err != nil {
			return err
		}
		// cache these as well
		if d.CachedFieldTerms == nil {
			d.CachedFieldTerms = make(map[string][]string)
		}
		d.CachedFieldTerms.Merge(fieldTerms)
	}

	// compute this hits sort value
	d.Sort = hc.sort.Value(d)

	// optimization, we track lowest sorting hit already removed from heap
	// with this one comparision, we can avoid all heap operations if
	// this hit would have been added and then immediately removed
	if hc.lowestMatchOutsideResults != nil {
		cmp := hc.sort.Compare(d, hc.lowestMatchOutsideResults)
		if cmp >= 0 {
			// this hit can't possibly be in the result set, so avoid heap ops
			ctx.DocumentMatchPool.Put(d)
			return nil
		}
	}

	heap.Push(hc, d)
	if hc.Len() > hc.size+hc.skip {
		removed := heap.Pop(hc).(*search.DocumentMatch)
		if hc.lowestMatchOutsideResults == nil {
			hc.lowestMatchOutsideResults = removed
		} else {
			cmp := hc.sort.Compare(removed, hc.lowestMatchOutsideResults)
			if cmp < 0 {
				tmp := hc.lowestMatchOutsideResults
				hc.lowestMatchOutsideResults = removed
				ctx.DocumentMatchPool.Put(tmp)
			}
		}
	}

	return nil
}

func (hc *HeapCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
}

// finalizeResults starts with the heap containing the final top size+skip
// it now throws away the results to be skipped
// and does final doc id lookup (if necessary)
func (hc *HeapCollector) finalizeResults(r index.IndexReader) error {
	count := hc.Len()
	size := count - hc.skip
	rv := make(search.DocumentMatchCollection, size)
	for count > 0 {
		count--

		if count >= hc.skip {
			size--
			doc := heap.Pop(hc).(*search.DocumentMatch)
			rv[size] = doc
			if doc.ID == "" {
				// look up the id since we need it for lookup
				var err error
				doc.ID, err = r.FinalizeDocID(doc.IndexInternalID)
				if err != nil {
					return err
				}
			}
		}
	}

	// no longer a heap
	hc.results = rv

	return nil
}

func (hc *HeapCollector) Results() search.DocumentMatchCollection {
	return hc.results
}

func (hc *HeapCollector) Total() uint64 {
	return hc.total
}

func (hc *HeapCollector) MaxScore() float64 {
	return hc.maxScore
}

func (hc *HeapCollector) Took() time.Duration {
	return hc.took
}

func (hc *HeapCollector) FacetResults() search.FacetResults {
	if hc.facetsBuilder != nil {
		return hc.facetsBuilder.Results()
	}
	return search.FacetResults{}
}

// heap interface implementation

func (hc *HeapCollector) Len() int {
	return len(hc.results)
}

func (hc *HeapCollector) Less(i, j int) bool {
	so := hc.sort.Compare(hc.results[i], hc.results[j])
	return -so < 0
}

func (hc *HeapCollector) Swap(i, j int) {
	hc.results[i], hc.results[j] = hc.results[j], hc.results[i]
}

func (hc *HeapCollector) Push(x interface{}) {
	hc.results = append(hc.results, x.(*search.DocumentMatch))
}

func (hc *HeapCollector) Pop() interface{} {
	var rv *search.DocumentMatch
	rv, hc.results = hc.results[len(hc.results)-1], hc.results[:len(hc.results)-1]
	return rv
}
