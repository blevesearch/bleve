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

package bleve

import (
	"context"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

type indexAliasImpl struct {
	name    string
	indexes []Index
	mutex   sync.RWMutex
	open    bool
}

// NewIndexAlias creates a new IndexAlias over the provided
// Index objects.
func NewIndexAlias(indexes ...Index) *indexAliasImpl {
	return &indexAliasImpl{
		name:    "alias",
		indexes: indexes,
		open:    true,
	}
}

// VisitIndexes invokes the visit callback on every
// indexes included in the index alias.
func (i *indexAliasImpl) VisitIndexes(visit func(Index)) {
	i.mutex.RLock()
	for _, idx := range i.indexes {
		visit(idx)
	}
	i.mutex.RUnlock()
}

func (i *indexAliasImpl) isAliasToSingleIndex() error {
	if len(i.indexes) < 1 {
		return ErrorAliasEmpty
	} else if len(i.indexes) > 1 {
		return ErrorAliasMulti
	}
	return nil
}

func (i *indexAliasImpl) Index(id string, data interface{}) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return err
	}

	return i.indexes[0].Index(id, data)
}

func (i *indexAliasImpl) Delete(id string) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return err
	}

	return i.indexes[0].Delete(id)
}

func (i *indexAliasImpl) Batch(b *Batch) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return err
	}

	return i.indexes[0].Batch(b)
}

func (i *indexAliasImpl) Document(id string) (index.Document, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil, err
	}

	return i.indexes[0].Document(id)
}

func (i *indexAliasImpl) DocCount() (uint64, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	rv := uint64(0)

	if !i.open {
		return 0, ErrorIndexClosed
	}

	for _, index := range i.indexes {
		otherCount, err := index.DocCount()
		if err == nil {
			rv += otherCount
		}
		// tolerate errors to produce partial counts
	}

	return rv, nil
}

func (i *indexAliasImpl) Search(req *SearchRequest) (*SearchResult, error) {
	return i.SearchInContext(context.Background(), req)
}

func (i *indexAliasImpl) SearchInContext(ctx context.Context, req *SearchRequest) (*SearchResult, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	if len(i.indexes) < 1 {
		return nil, ErrorAliasEmpty
	}
	if _, ok := ctx.Value(search.PreSearchKey).(bool); ok {
		// since presearchKey is set, it means that the request
		// is being executed as part of a presearch, which
		// indicates that this index alias is set as an Index
		// in another alias, so we need to do a presearch search
		// and NOT a real search
		return preSearchDataSearch(ctx, req, i.indexes...)
	}

	// at this point we know we are doing a real search
	// either after a presearch is done, or directly
	// on the alias

	// check if request has preSearchData which would indicate that the
	// request has already been preSearched and we can skip the
	// preSearch step now, we call an optional function to
	// redistribute the preSearchData to the individual indexes
	// if necessary
	var preSearchData map[string]map[string]interface{}
	if req.PreSearchData != nil {
		if requestHasKNN(req) {
			var err error
			preSearchData, err = redistributeKNNPreSearchData(req, i.indexes)
			if err != nil {
				return nil, err
			}
		}
	}

	// short circuit the simple case
	if len(i.indexes) == 1 {
		if preSearchData != nil {
			req.PreSearchData = preSearchData[i.indexes[0].Name()]
		}
		return i.indexes[0].SearchInContext(ctx, req)
	}

	// at this stage we know we have multiple indexes
	// check if preSearchData needs to be gathered from all indexes
	// before executing the query
	var err error
	// only perform presearch if
	//  - the request does not already have preSearchData
	//  - the request requires presearch
	var preSearchDuration time.Duration
	var sr *SearchResult
	if req.PreSearchData == nil && preSearchRequired(req) {
		searchStart := time.Now()
		preSearchResult, err := preSearch(ctx, req, i.indexes...)
		if err != nil {
			return nil, err
		}
		// check if the presearch result has any errors and if so
		// return the search result as is without executing the query
		// so that the errors are not lost
		if preSearchResult.Status.Failed > 0 {
			return preSearchResult, nil
		}

		// if there are no errors, then merge the data in the presearch result
		preSearchResult = mergePreSearchResult(req, preSearchResult, i.indexes)
		if requestSatisfiedByPreSearch(req) {
			sr = finalizeSearchResult(req, preSearchResult)
			// no need to run the 2nd phase MultiSearch(..)
		} else {
			preSearchData, err = constructPreSearchData(req, preSearchResult, i.indexes)
			if err != nil {
				return nil, err
			}
		}
		preSearchDuration = time.Since(searchStart)
	}

	// check if search result was generated as part of presearch itself
	if sr == nil {
		sr, err = MultiSearch(ctx, req, preSearchData, i.indexes...)
		if err != nil {
			return nil, err
		}
	}
	sr.Took += preSearchDuration
	return sr, nil
}

func (i *indexAliasImpl) Fields() ([]string, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil, err
	}

	return i.indexes[0].Fields()
}

func (i *indexAliasImpl) FieldDict(field string) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := i.indexes[0].FieldDict(field)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexAliasImplFieldDict{
		index:     i,
		fieldDict: fieldDict,
	}, nil
}

func (i *indexAliasImpl) FieldDictRange(field string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := i.indexes[0].FieldDictRange(field, startTerm, endTerm)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexAliasImplFieldDict{
		index:     i,
		fieldDict: fieldDict,
	}, nil
}

func (i *indexAliasImpl) FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := i.indexes[0].FieldDictPrefix(field, termPrefix)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexAliasImplFieldDict{
		index:     i,
		fieldDict: fieldDict,
	}, nil
}

func (i *indexAliasImpl) Close() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.open = false
	return nil
}

func (i *indexAliasImpl) Mapping() mapping.IndexMapping {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].Mapping()
}

func (i *indexAliasImpl) Stats() *IndexStat {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].Stats()
}

func (i *indexAliasImpl) StatsMap() map[string]interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].StatsMap()
}

func (i *indexAliasImpl) GetInternal(key []byte) ([]byte, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil, err
	}

	return i.indexes[0].GetInternal(key)
}

func (i *indexAliasImpl) SetInternal(key, val []byte) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return err
	}

	return i.indexes[0].SetInternal(key, val)
}

func (i *indexAliasImpl) DeleteInternal(key []byte) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return err
	}

	return i.indexes[0].DeleteInternal(key)
}

func (i *indexAliasImpl) Advanced() (index.Index, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil, err
	}

	return i.indexes[0].Advanced()
}

func (i *indexAliasImpl) Add(indexes ...Index) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.indexes = append(i.indexes, indexes...)
}

func (i *indexAliasImpl) removeSingle(index Index) {
	for pos, in := range i.indexes {
		if in == index {
			i.indexes = append(i.indexes[:pos], i.indexes[pos+1:]...)
			break
		}
	}
}

func (i *indexAliasImpl) Remove(indexes ...Index) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for _, in := range indexes {
		i.removeSingle(in)
	}
}

func (i *indexAliasImpl) Swap(in, out []Index) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// add
	i.indexes = append(i.indexes, in...)

	// delete
	for _, ind := range out {
		i.removeSingle(ind)
	}
}

// createChildSearchRequest creates a separate
// request from the original
// For now, avoid data race on req structure.
// TODO disable highlight/field load on child
// requests, and add code to do this only on
// the actual final results.
// Perhaps that part needs to be optional,
// could be slower in remote usages.
func createChildSearchRequest(req *SearchRequest, preSearchData map[string]interface{}) *SearchRequest {
	return copySearchRequest(req, preSearchData)
}

type asyncSearchResult struct {
	Name   string
	Result *SearchResult
	Err    error
}

func preSearchRequired(req *SearchRequest) bool {
	return requestHasKNN(req)
}

func preSearch(ctx context.Context, req *SearchRequest, indexes ...Index) (*SearchResult, error) {
	// create a dummy request with a match none query
	// since we only care about the preSearchData in PreSearch
	dummyRequest := &SearchRequest{
		Query: query.NewMatchNoneQuery(),
	}
	newCtx := context.WithValue(ctx, search.PreSearchKey, true)
	if requestHasKNN(req) {
		addKnnToDummyRequest(dummyRequest, req)
	}
	return preSearchDataSearch(newCtx, dummyRequest, indexes...)
}

func tagHitsWithIndexName(sr *SearchResult, indexName string) {
	for _, hit := range sr.Hits {
		hit.IndexNames = append(hit.IndexNames, indexName)
	}
}

// if the request is satisfied by just the presearch result,
// finalize the result and return it directly without
// performing multi search
func finalizeSearchResult(req *SearchRequest, preSearchResult *SearchResult) *SearchResult {
	if preSearchResult == nil {
		return nil
	}

	// global values across all hits irrespective of pagination settings
	preSearchResult.Total = uint64(preSearchResult.Hits.Len())
	maxScore := float64(0)
	for i, hit := range preSearchResult.Hits {
		// since we are now using the presearch result as the final result
		// we can discard the indexNames from the hits as they are no longer
		// relevant.
		hit.IndexNames = nil
		if hit.Score > maxScore {
			maxScore = hit.Score
		}
		hit.HitNumber = uint64(i)
	}
	preSearchResult.MaxScore = maxScore
	// now apply pagination settings
	var reverseQueryExecution bool
	if req.SearchBefore != nil {
		reverseQueryExecution = true
		req.Sort.Reverse()
		req.SearchAfter = req.SearchBefore
	}
	if req.SearchAfter != nil {
		preSearchResult.Hits = collector.FilterHitsBySearchAfter(preSearchResult.Hits, req.Sort, req.SearchAfter)
	}
	preSearchResult.Hits = hitsInCurrentPage(req, preSearchResult.Hits)
	if reverseQueryExecution {
		// reverse the sort back to the original
		req.Sort.Reverse()
		// resort using the original order
		mhs := newSearchHitSorter(req.Sort, preSearchResult.Hits)
		req.SortFunc()(mhs)
		req.SearchAfter = nil
	}

	if req.Explain {
		preSearchResult.Request = req
	}
	return preSearchResult
}

func mergePreSearchResult(req *SearchRequest, res *SearchResult,
	indexes []Index) *SearchResult {
	if requestHasKNN(req) {
		res.Hits = mergeKNNDocumentMatches(req, res.Hits)
	}
	return res
}

func requestSatisfiedByPreSearch(req *SearchRequest) bool {
	if requestHasKNN(req) && isKNNrequestSatisfiedByPreSearch(req) {
		return true
	}
	return false
}

func constructPreSearchData(req *SearchRequest, preSearchResult *SearchResult, indexes []Index) (map[string]map[string]interface{}, error) {
	mergedOut := make(map[string]map[string]interface{}, len(indexes))
	for _, index := range indexes {
		mergedOut[index.Name()] = make(map[string]interface{})
	}
	var err error
	if requestHasKNN(req) {
		mergedOut, err = constructKnnPresearchData(mergedOut, preSearchResult, indexes)
		if err != nil {
			return nil, err
		}
	}
	return mergedOut, nil
}

func preSearchDataSearch(ctx context.Context, req *SearchRequest, indexes ...Index) (*SearchResult, error) {
	asyncResults := make(chan *asyncSearchResult, len(indexes))

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(in Index, childReq *SearchRequest) {
		rv := asyncSearchResult{Name: in.Name()}
		rv.Result, rv.Err = in.SearchInContext(ctx, childReq)
		asyncResults <- &rv
		waitGroup.Done()
	}

	waitGroup.Add(len(indexes))
	for _, in := range indexes {
		go searchChildIndex(in, createChildSearchRequest(req, nil))
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(asyncResults)
	}()

	var sr *SearchResult
	indexErrors := make(map[string]error)

	for asr := range asyncResults {
		if asr.Err == nil {
			if sr == nil {
				// first result
				sr = asr.Result
				tagHitsWithIndexName(sr, asr.Name)
			} else {
				// merge with previous
				tagHitsWithIndexName(asr.Result, asr.Name)
				sr.Merge(asr.Result)
			}
		} else {
			indexErrors[asr.Name] = asr.Err
		}
	}

	// merge just concatenated all the hits
	// now lets clean it up

	// handle case where no results were successful
	if sr == nil {
		sr = &SearchResult{
			Status: &SearchStatus{
				Errors: make(map[string]error),
			},
		}
	}

	// in presearch partial results are not allowed as it can lead to
	// the real search giving incorrect results, and hence the search
	// result is reset.
	// discard partial hits if some child index has failed or
	// if some child alias has returned partial results.
	if len(indexErrors) > 0 || sr.Status.Failed > 0 {
		sr = &SearchResult{
			Status: sr.Status,
		}
		if sr.Status.Errors == nil {
			sr.Status.Errors = make(map[string]error)
		}
		for indexName, indexErr := range indexErrors {
			sr.Status.Errors[indexName] = indexErr
			sr.Status.Total++
			sr.Status.Failed++
		}
	}

	return sr, nil
}

// hitsInCurrentPage returns the hits in the current page
// using the From and Size parameters in the request
func hitsInCurrentPage(req *SearchRequest, hits []*search.DocumentMatch) []*search.DocumentMatch {
	sortFunc := req.SortFunc()
	// sort all hits with the requested order
	if len(req.Sort) > 0 {
		sorter := newSearchHitSorter(req.Sort, hits)
		sortFunc(sorter)
	}
	// now skip over the correct From
	if req.From > 0 && len(hits) > req.From {
		hits = hits[req.From:]
	} else if req.From > 0 {
		hits = search.DocumentMatchCollection{}
	}
	// now trim to the correct size
	if req.Size > 0 && len(hits) > req.Size {
		hits = hits[0:req.Size]
	}
	return hits
}

// MultiSearch executes a SearchRequest across multiple Index objects,
// then merges the results.  The indexes must honor any ctx deadline.
func MultiSearch(ctx context.Context, req *SearchRequest, preSearchData map[string]map[string]interface{}, indexes ...Index) (*SearchResult, error) {

	searchStart := time.Now()
	asyncResults := make(chan *asyncSearchResult, len(indexes))

	var reverseQueryExecution bool
	if req.SearchBefore != nil {
		reverseQueryExecution = true
		req.Sort.Reverse()
		req.SearchAfter = req.SearchBefore
		req.SearchBefore = nil
	}

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(in Index, childReq *SearchRequest) {
		rv := asyncSearchResult{Name: in.Name()}
		rv.Result, rv.Err = in.SearchInContext(ctx, childReq)
		asyncResults <- &rv
		waitGroup.Done()
	}

	waitGroup.Add(len(indexes))
	for _, in := range indexes {
		var payload map[string]interface{}
		if preSearchData != nil {
			payload = preSearchData[in.Name()]
		}
		go searchChildIndex(in, createChildSearchRequest(req, payload))
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(asyncResults)
	}()

	var sr *SearchResult
	indexErrors := make(map[string]error)

	for asr := range asyncResults {
		if asr.Err == nil {
			if sr == nil {
				// first result
				sr = asr.Result
			} else {
				// merge with previous
				sr.Merge(asr.Result)
			}
		} else {
			indexErrors[asr.Name] = asr.Err
		}
	}

	// merge just concatenated all the hits
	// now lets clean it up

	// handle case where no results were successful
	if sr == nil {
		sr = &SearchResult{
			Status: &SearchStatus{
				Errors: make(map[string]error),
			},
		}
	}

	sr.Hits = hitsInCurrentPage(req, sr.Hits)

	// fix up facets
	for name, fr := range req.Facets {
		sr.Facets.Fixup(name, fr.Size)
	}

	if reverseQueryExecution {
		// reverse the sort back to the original
		req.Sort.Reverse()
		// resort using the original order
		mhs := newSearchHitSorter(req.Sort, sr.Hits)
		req.SortFunc()(mhs)
		// reset request
		req.SearchBefore = req.SearchAfter
		req.SearchAfter = nil
	}

	// fix up original request
	if req.Explain {
		sr.Request = req
	}
	searchDuration := time.Since(searchStart)
	sr.Took = searchDuration

	// fix up errors
	if len(indexErrors) > 0 {
		if sr.Status.Errors == nil {
			sr.Status.Errors = make(map[string]error)
		}
		for indexName, indexErr := range indexErrors {
			sr.Status.Errors[indexName] = indexErr
			sr.Status.Total++
			sr.Status.Failed++
		}
	}

	return sr, nil
}

func (i *indexAliasImpl) NewBatch() *Batch {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].NewBatch()
}

func (i *indexAliasImpl) Name() string {
	return i.name
}

func (i *indexAliasImpl) SetName(name string) {
	i.name = name
}

type indexAliasImplFieldDict struct {
	index     *indexAliasImpl
	fieldDict index.FieldDict
}

func (f *indexAliasImplFieldDict) BytesRead() uint64 {
	return f.fieldDict.BytesRead()
}

func (f *indexAliasImplFieldDict) Next() (*index.DictEntry, error) {
	return f.fieldDict.Next()
}

func (f *indexAliasImplFieldDict) Close() error {
	defer f.index.mutex.RUnlock()
	return f.fieldDict.Close()
}
