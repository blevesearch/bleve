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

package collector

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeTopNCollector int

func init() {
	var coll TopNCollector
	reflectStaticSizeTopNCollector = int(reflect.TypeOf(coll).Size())
}

type collectorStore interface {
	// Add the document, and if the new store size exceeds the provided size
	// the last element is removed and returned.  If the size has not been
	// exceeded, nil is returned.
	AddNotExceedingSize(doc *search.DocumentMatch, size int) []*search.DocumentMatch

	PopWhile(popCondition func(doc *search.DocumentMatch) bool) []*search.DocumentMatch

	Add(doc *search.DocumentMatch)

	Remove() *search.DocumentMatch

	Len() int

	Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error)
}

// PreAllocSizeSkipCap will cap preallocation to this amount when
// size+skip exceeds this value
var PreAllocSizeSkipCap = 1000

type collectorCompare func(i, j *search.DocumentMatch) int

type collectorFixup func(d *search.DocumentMatch) error

// TopNCollector collects the top N hits, optionally skipping some results
type TopNCollector struct {
	size          int
	skip          int
	total         uint64
	bytesRead     uint64
	maxScore      float64
	took          time.Duration
	sort          search.SortOrder
	results       search.DocumentMatchCollection
	facetsBuilder *search.FacetsBuilder

	store collectorStore

	needDocIds    bool
	neededFields  []string
	cachedScoring []bool
	cachedDesc    []bool

	lowestMatchOutsideResults *search.DocumentMatch
	updateFieldVisitor        index.DocValueVisitor
	dvReader                  index.DocValueReader
	searchAfter               *search.DocumentMatch
}

// CheckDoneEvery controls how frequently we check the context deadline
const CheckDoneEvery = uint64(1024)

// NewTopNCollector builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewTopNCollector(size int, skip int, sort search.SortOrder) *TopNCollector {
	return newTopNCollector(size, skip, sort)
}

// NewTopNCollectorAfter builds a collector to find the top 'size' hits
// skipping over the first 'skip' hits
// ordering hits by the provided sort order
func NewTopNCollectorAfter(size int, sort search.SortOrder, after []string) *TopNCollector {
	rv := newTopNCollector(size, 0, sort)
	rv.searchAfter = &search.DocumentMatch{
		Sort: after,
	}

	for pos, ss := range sort {
		if ss.RequiresDocID() {
			rv.searchAfter.ID = after[pos]
		}
		if ss.RequiresScoring() {
			if score, err := strconv.ParseFloat(after[pos], 64); err == nil {
				rv.searchAfter.Score = score
			}
		}
	}

	return rv
}

func newTopNCollector(size int, skip int, sort search.SortOrder) *TopNCollector {
	hc := &TopNCollector{size: size, skip: skip, sort: sort}
	hc.store = getOptimalCollectorStore(size, skip, func(i, j *search.DocumentMatch) int {
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

func (hc *TopNCollector) Size() int {
	sizeInBytes := reflectStaticSizeTopNCollector + size.SizeOfPtr

	if hc.facetsBuilder != nil {
		sizeInBytes += hc.facetsBuilder.Size()
	}

	for _, entry := range hc.neededFields {
		sizeInBytes += len(entry) + size.SizeOfString
	}

	sizeInBytes += len(hc.cachedScoring) + len(hc.cachedDesc)

	return sizeInBytes
}

func getOptimalCollectorStore(size, skip int, comparator collectorCompare) collectorStore {
	// pre-allocate space on the store to avoid reslicing
	// unless the size + skip is too large, then cap it
	// everything should still work, just reslices as necessary
	backingSize := size + skip + 1
	if size+skip > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}

	if size+skip > 10 {
		return newStoreHeap(backingSize, comparator)
	} else {
		return newStoreSlice(backingSize, comparator)
	}
}

// Collect goes to the index to find the matching documents
func (hc *TopNCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	// pre-allocate enough space in the DocumentMatchPool
	// unless the size + skip is too large, then cap it
	// everything should still work, just allocates DocumentMatches on demand
	backingSize := hc.size + hc.skip + 1
	if hc.size+hc.skip > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(backingSize+searcher.DocumentMatchPoolSize(), len(hc.sort)),
		Collector:         hc,
		IndexReader:       reader,
	}

	hc.dvReader, err = reader.DocValueReader(hc.neededFields)
	if err != nil {
		return err
	}

	hc.updateFieldVisitor = func(field string, term []byte) {
		if hc.facetsBuilder != nil {
			hc.facetsBuilder.UpdateVisitor(field, term)
		}
		hc.sort.UpdateVisitor(field, term)
	}

	dmHandlerMaker := MakeTopNDocumentMatchHandler
	if cv := ctx.Value(search.MakeDocumentMatchHandlerKey); cv != nil {
		dmHandlerMaker = cv.(search.MakeDocumentMatchHandler)
	}
	// use the application given builder for making the custom document match
	// handler and perform callbacks/invocations on the newly made handler.
	dmHandler, loadID, err := dmHandlerMaker(searchContext)
	if err != nil {
		return err
	}

	hc.needDocIds = hc.needDocIds || loadID
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

		err = hc.prepareDocumentMatch(searchContext, reader, next)
		if err != nil {
			break
		}

		err = dmHandler(next)
		if err != nil {
			break
		}

		next, err = searcher.Next(searchContext)
	}

	statsCallbackFn := ctx.Value(search.SearchIOStatsCallbackKey)
	if statsCallbackFn != nil {
		// hc.bytesRead corresponds to the
		// total bytes read as part of docValues being read every hit
		// which must be accounted by invoking the callback.
		statsCallbackFn.(search.SearchIOStatsCallbackFunc)(hc.bytesRead)

		search.RecordSearchCost(ctx, search.AddM, hc.bytesRead)
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

var sortByScoreOpt = []string{"_score"}

func (hc *TopNCollector) prepareDocumentMatch(ctx *search.SearchContext,
	reader index.IndexReader, d *search.DocumentMatch) (err error) {

	// visit field terms for features that require it (sort, facets)
	if len(hc.neededFields) > 0 {
		err = hc.visitFieldTerms(reader, d)
		if err != nil {
			return err
		}
	}

	// increment total hits
	hc.total++
	d.HitNumber = hc.total

	// update max score
	if d.Score > hc.maxScore {
		hc.maxScore = d.Score
	}

	// see if we need to load ID (at this early stage, for example to sort on it)
	if hc.needDocIds {
		d.ID, err = reader.ExternalID(d.IndexInternalID)
		if err != nil {
			return err
		}
	}

	// compute this hits sort value
	if len(hc.sort) == 1 && hc.cachedScoring[0] {
		d.Sort = sortByScoreOpt
	} else {
		hc.sort.Value(d)
	}

	return nil
}

func MakeTopNDocumentMatchHandler(
	ctx *search.SearchContext) (search.DocumentMatchHandler, bool, error) {
	var hc *TopNCollector
	var ok bool
	if hc, ok = ctx.Collector.(*TopNCollector); ok {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				return nil
			}

			// support search after based pagination,
			// if this hit is <= the search after sort key
			// we should skip it
			if hc.searchAfter != nil {
				// exact sort order matches use hit number to break tie
				// but we want to allow for exact match, so we pretend
				hc.searchAfter.HitNumber = d.HitNumber
				if hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, d, hc.searchAfter) <= 0 {
					return nil
				}
			}

			// optimization, we track lowest sorting hit already removed from heap
			// with this one comparison, we can avoid all heap operations if
			// this hit would have been added and then immediately removed
			if hc.lowestMatchOutsideResults != nil {
				cmp := hc.sort.Compare(hc.cachedScoring, hc.cachedDesc, d,
					hc.lowestMatchOutsideResults)
				if cmp >= 0 {
					// this hit can't possibly be in the result set, so avoid heap ops
					ctx.DocumentMatchPool.Put(d)
					return nil
				}
			}

			removedDocs := hc.store.AddNotExceedingSize(d, hc.size+hc.skip)
			for _, removed := range removedDocs {
				if hc.lowestMatchOutsideResults == nil {
					hc.lowestMatchOutsideResults = removed
				} else {
					cmp := hc.sort.Compare(hc.cachedScoring, hc.cachedDesc,
						removed, hc.lowestMatchOutsideResults)
					if cmp < 0 {
						tmp := hc.lowestMatchOutsideResults
						hc.lowestMatchOutsideResults = removed
						ctx.DocumentMatchPool.Put(tmp)
					}
				}
			}
			return nil
		}, false, nil
	}
	return nil, false, nil
}

// visitFieldTerms is responsible for visiting the field terms of the
// search hit, and passing visited terms to the sort and facet builder
func (hc *TopNCollector) visitFieldTerms(reader index.IndexReader, d *search.DocumentMatch) error {
	if hc.facetsBuilder != nil {
		hc.facetsBuilder.StartDoc()
	}

	err := hc.dvReader.VisitDocValues(d.IndexInternalID, hc.updateFieldVisitor)
	if hc.facetsBuilder != nil {
		hc.facetsBuilder.EndDoc()
	}

	hc.bytesRead += hc.dvReader.BytesRead()

	return err
}

// SetFacetsBuilder registers a facet builder for this collector
func (hc *TopNCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
	fieldsRequiredForFaceting := facetsBuilder.RequiredFields()
	// for each of these fields, append only if not already there in hc.neededFields.
	for _, field := range fieldsRequiredForFaceting {
		found := false
		for _, neededField := range hc.neededFields {
			if field == neededField {
				found = true
				break
			}
		}
		if !found {
			hc.neededFields = append(hc.neededFields, field)
		}
	}
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
			doc.ID, err = r.ExternalID(doc.IndexInternalID)
			if err != nil {
				return err
			}
		}
		doc.Complete(nil)
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

func (hc *TopNCollector) SetKNNCollectorStore(kArray []int64, scoreCorrector func(docMatch *search.DocumentMatch)) {
	// background - assuming D segments exists in the index (D is a highly variable number)
	// the vector reader returns the top K results for each segment
	// resulting in D * K hits matching the KNN query. With M KNN queries,
	// the total number of results matching the KNN queries is at max D * M * K
	// the goal is to ensure that the behavior remains the same as observed
	// in a single segment scenario - M * K results are returned for M KNN queries
	//
	// the scenario becomes complex when pagination is involved - since the pagination is done on the total score
	// the total score is a combination of TF-IDF score and KNN scores and as the document matches
	// are streamed from the compound searcher across Next() calls,
	// attempting to retain the top K elements for each KNN query with accurate pagination
	// becomes a challenge - since for each KNN query we need to reduce the KNN score
	// of D*K - K = K*(D-1) hits to 0, which reduces the total score of the document
	// which inturn messes up the pagination - since the total score is used for pagination
	//
	// this problem is a constrained equivalent to the problem of maintaining top N elements from an infinite
	// stream of objects where the stream can contain duplicates, (wherein we need to
	// update the value of the duplicate object received in the stream), with the additional
	// constraint that the space complexity of the solution should be O(N) and not infinite
	//
	// idea:-
	// keep a min-heap for each KNN query 	- this is because we need to maintain top K elements for each KNN query
	// keep a min-heap for the TF-IDF query - this is because we need to maintain top size+from elements for the TF-IDF query
	//										  this is to ensure that when ejecting a document from the total score heap
	//										  it is guaranteed that the document will never be part of the top size+from elements
	// keep a min-heap for the total score 	- this is because the final pagination is done on the total score
	//										  this heap is voluntarily corrupted as documents are ejected out of the KNN heaps
	//										  but we use the TF-IDF heap to ensure that the document ejected out of the total score heap
	//										  is guaranteed to be outside the top size+from elements
	// all the knn heaps and TF-IDF heaps can be completely disjoint and hence the total score heap
	// served as a catch all heap for all the document matches. Only ejecting a document if and only if
	// the min score document at the root is having a score 0.0
	//
	// as per the design the sorting of the document matches is done
	// only after all the document matches are collected from the compound searcher

	internalHeaps := make([]collectorStore, len(kArray))
	// index 0 of kArray is the size+from for the TF-IDF searcher
	// index 1 of kArray is the K for the KNN searcher at index 0 of SearchRequest.KNN slice
	// index 2 of kArray is the K for the KNN searcher at index 1 of SearchRequest.KNN slice
	for knnIdx, k := range kArray {
		// TODO - Check if the datatype of k can be made into an int instead of int64
		idx := knnIdx
		internalHeaps[knnIdx] = getOptimalCollectorStore(int(k), 0, func(i, j *search.DocumentMatch) int {
			if i.ScoreBreakdown[idx] < j.ScoreBreakdown[idx] {
				// min heap to maintain top K elements for KNN searcher or the top from+size values of the Query searcher
				return 1
			}
			return -1
		})
	}
	totalScoreHeap := getOptimalCollectorStore(hc.size, hc.skip, func(i, j *search.DocumentMatch) int {
		if i.Score < j.Score {
			// min heap to maintain top size+from elements for the total score
			return 1
		}
		return -1
	})
	hc.store = newStoreKNN(totalScoreHeap, kArray, internalHeaps, scoreCorrector)
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
	return nil
}
