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
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"golang.org/x/net/context"
)

type collectorCompare func(i, j *search.DocumentMatch) int

type collectorFixup func(d *search.DocumentMatch) error

// TopNCollector collects the top N hits, optionally skipping some results
type TopNCollector struct {
	size          int
	skip          int
	total         uint64
	maxScore      float64
	took          time.Duration
	sort          search.SortOrder
	results       search.DocumentMatchCollection
	facetsBuilder *search.FacetsBuilder

	store *collectStoreList

	needDocIds    bool
	neededFields  []string
	cachedScoring []bool
	cachedDesc    []bool

	lowestMatchOutsideResults *search.DocumentMatch
}

// CheckDoneEvery controls how frequently we check the context deadline
const CheckDoneEvery = uint64(1024)

// NewTopNCollector builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewTopNCollector(size int, skip int, sort search.SortOrder) *TopNCollector {
	hc := &TopNCollector{size: size, skip: skip, sort: sort}
	// pre-allocate space on the heap, we need size+skip results
	// +1 additional while figuring out which to evict
	hc.store = newStoreList(size+skip+1, func(i, j *search.DocumentMatch) int {
		return hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, i, j)
	})

	// these lookups traverse an interface, so do once up-front
	if sort.RequiresDocID() {
		hc.needDocIds = true
	}
	hc.neededFields = sort.RequiredFields()
	hc.cachedScoring = sort.CacheIsScore()
	hc.cachedDesc = sort.CacheDescending()

	return hc
}

// Collect goes to the index to find the matching documents
func (hc *TopNCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// search context with enough pre-allocated document matches
	// we keep references to size+skip ourselves
	// plus possibly one extra for the highestMatchOutsideResults
	// plus the amount required by the searcher tree
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(hc.size+hc.skip+1+searcher.DocumentMatchPoolSize(), len(hc.sort)),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if hc.total%CheckDoneEvery == 0 {
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

var sortByScoreOpt = []string{"_score"}

func (hc *TopNCollector) collectSingle(ctx *search.SearchContext, reader index.IndexReader, d *search.DocumentMatch) error {
	// increment total hits
	hc.total++
	d.HitNumber = hc.total

	// update max score
	if d.Score > hc.maxScore {
		hc.maxScore = d.Score
	}

	var err error
	// see if we need to load ID (at this early stage, for example to sort on it)
	if hc.needDocIds {
		d.ID, err = reader.FinalizeDocID(d.IndexInternalID)
		if err != nil {
			return err
		}
	}

	// see if we need to load the stored fields
	if len(hc.neededFields) > 0 {
		// find out which fields haven't been loaded yet
		fieldsToLoad := d.CachedFieldTerms.FieldsNotYetCached(hc.neededFields)
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
	if len(hc.sort) == 1 && hc.cachedScoring[0] {
		d.Sort = sortByScoreOpt
	} else {
		hc.sort.Value(d)
	}

	// optimization, we track lowest sorting hit already removed from heap
	// with this one comparision, we can avoid all heap operations if
	// this hit would have been added and then immediately removed
	if hc.lowestMatchOutsideResults != nil {
		cmp := hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, d, hc.lowestMatchOutsideResults)
		if cmp >= 0 {
			// this hit can't possibly be in the result set, so avoid heap ops
			ctx.DocumentMatchPool.Put(d)
			return nil
		}
	}

	hc.store.Add(d)
	if hc.store.Len() > hc.size+hc.skip {
		removed := hc.store.RemoveLast()
		if hc.lowestMatchOutsideResults == nil {
			hc.lowestMatchOutsideResults = removed
		} else {
			cmp := hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, removed, hc.lowestMatchOutsideResults)
			if cmp < 0 {
				tmp := hc.lowestMatchOutsideResults
				hc.lowestMatchOutsideResults = removed
				ctx.DocumentMatchPool.Put(tmp)
			}
		}
	}

	return nil
}

// SetFacetsBuilder registers a facet builder for this collector
func (hc *TopNCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
}

// finalizeResults starts with the heap containing the final top size+skip
// it now throws away the results to be skipped
// and does final doc id lookup (if necessary)
func (hc *TopNCollector) finalizeResults(r index.IndexReader) error {
	var err error
	hc.results, err = hc.store.Final(hc.skip, func(doc *search.DocumentMatch) error {
		if doc.ID == "" {
			// look up the id since we need it for lookup
			var err error
			doc.ID, err = r.FinalizeDocID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// Results returns the collected hits
func (hc *TopNCollector) Results() search.DocumentMatchCollection {
	return hc.results
}

// Total returns the total number of hits
func (hc *TopNCollector) Total() uint64 {
	return hc.total
}

// MaxScore returns the maximum score seen across all the hits
func (hc *TopNCollector) MaxScore() float64 {
	return hc.maxScore
}

// Took returns the time spent collecting hits
func (hc *TopNCollector) Took() time.Duration {
	return hc.took
}

// FacetResults returns the computed facets results
func (hc *TopNCollector) FacetResults() search.FacetResults {
	if hc.facetsBuilder != nil {
		return hc.facetsBuilder.Results()
	}
	return search.FacetResults{}
}
