//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"sort"
	"sync"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/search"
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

func (i *indexAliasImpl) Document(id string) (*document.Document, error) {
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
		if err != nil {
			return 0, err
		}
		rv += otherCount
	}

	return rv, nil
}

func (i *indexAliasImpl) Search(req *SearchRequest) (*SearchResult, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	if len(i.indexes) < 1 {
		return nil, ErrorAliasEmpty
	}

	// short circuit the simple case
	if len(i.indexes) == 1 {
		rel, err := i.indexes[0].Search(req)
		if req.PartialResultsOk {
			if rel.Pr == nil {
				pr := &PartialResults{}
				if err == nil {
					pr.Successful = 1
				} else {
					pr.Failed = 1
				}
				rel.Pr = pr
			}
		}
		return rel, err
	}

	return MultiSearch(req, i.indexes...)
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

func (i *indexAliasImpl) DumpAll() chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].DumpAll()
}

func (i *indexAliasImpl) DumpDoc(id string) chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].DumpDoc(id)
}

func (i *indexAliasImpl) DumpFields() chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil
	}

	return i.indexes[0].DumpFields()
}

func (i *indexAliasImpl) Close() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.open = false
	return nil
}

func (i *indexAliasImpl) Mapping() *IndexMapping {
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

func (i *indexAliasImpl) Advanced() (index.Index, store.KVStore, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, nil, ErrorIndexClosed
	}

	err := i.isAliasToSingleIndex()
	if err != nil {
		return nil, nil, err
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
func createChildSearchRequest(req *SearchRequest) *SearchRequest {
	rv := SearchRequest{
		Query:     req.Query,
		Size:      req.Size + req.From,
		From:      0,
		Highlight: req.Highlight,
		Fields:    req.Fields,
		Facets:    req.Facets,
		Explain:   req.Explain,
	}
	return &rv
}

// MultiSearch executes a SearchRequest across multiple
// Index objects, then merges the results.
func MultiSearch(req *SearchRequest, indexes ...Index) (*SearchResult, error) {
	searchStart := time.Now()
	results := make(chan *SearchResult)
	errs := make(chan error)

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(waitGroup *sync.WaitGroup, in Index, results chan *SearchResult, errs chan error) {
		go func() {
			defer waitGroup.Done()
			childReq := createChildSearchRequest(req)
			searchResult, err := in.Search(childReq)
			if err != nil {
				errs <- err
			} else {
				results <- searchResult
			}
		}()
	}

	for _, in := range indexes {
		waitGroup.Add(1)
		searchChildIndex(&waitGroup, in, results, errs)
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(results)
		close(errs)
	}()

	var sr *SearchResult
	var err error
	var result *SearchResult
	pr := &PartialResults{}
	ok := true
	for ok {
		select {
		case result, ok = <-results:
			if ok {
				if req.PartialResultsOk {
					if result.Pr != nil &&
						(result.Pr.Failed > 0 ||
							result.Pr.Partial > 0) {
						pr.Partial++
					} else {
						pr.Successful++
					}
				}
				if sr == nil {
					// first result
					sr = result
				} else {
					// merge with previous
					sr.Merge(result)
				}
			}
		case err, ok = <-errs:
			// for now stop on any error
			// FIXME offer other behaviors
			if err != nil {
				if req.PartialResultsOk {
					pr.Failed++
				} else {
					return nil, err
				}
			}
		}
	}

	if req.PartialResultsOk {
		sr.Pr = pr
	}
	// merge just concatenated all the hits
	// now lets clean it up

	// first sort it by score
	sort.Sort(sr.Hits)

	// now skip over the correct From
	if req.From > 0 && len(sr.Hits) > req.From {
		sr.Hits = sr.Hits[req.From:]
	} else if req.From > 0 {
		sr.Hits = search.DocumentMatchCollection{}
	}

	// now trim to the correct size
	if req.Size > 0 && len(sr.Hits) > req.Size {
		sr.Hits = sr.Hits[0:req.Size]
	}

	// fix up facets
	for name, fr := range req.Facets {
		sr.Facets.Fixup(name, fr.Size)
	}

	// fix up original request
	sr.Request = req
	searchDuration := time.Since(searchStart)
	sr.Took = searchDuration

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

func (f *indexAliasImplFieldDict) Next() (*index.DictEntry, error) {
	return f.fieldDict.Next()
}

func (f *indexAliasImplFieldDict) Close() error {
	defer f.index.mutex.RUnlock()
	return f.fieldDict.Close()
}
