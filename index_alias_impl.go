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

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/search"
)

type indexAliasImpl struct {
	indexes []Index
	mutex   sync.RWMutex
	open    bool
}

// NewIndexAlias creates a new IndexAlias over the provided
// Index objects.
func NewIndexAlias(indexes ...Index) *indexAliasImpl {
	return &indexAliasImpl{
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

func (i *indexAliasImpl) Batch(b Batch) error {
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

func (i *indexAliasImpl) DocCount() uint64 {
	rv := uint64(0)

	if !i.open {
		return 0
	}

	for _, index := range i.indexes {
		rv += index.DocCount()
	}

	return rv
}

func (i *indexAliasImpl) Search(req *SearchRequest) (*SearchResult, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if len(i.indexes) < 1 {
		return nil, ErrorAliasEmpty
	}

	// short circuit the simple case
	if len(i.indexes) == 1 {
		return i.indexes[0].Search(req)
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

func (i *indexAliasImpl) Close() {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.open = false
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

// MultiSearch executes a SearchRequest across multiple
// Index objects, then merges the results.
func MultiSearch(req *SearchRequest, indexes ...Index) (*SearchResult, error) {
	results := make(chan *SearchResult)
	errs := make(chan error)

	// remember the original from and size
	size := req.Size
	req.Size += req.From
	from := req.From
	req.From = 0

	// FIXME should create new request for
	// child queries, disable highlighting
	// and field loading
	// then, perform these steps only on
	// the final results

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	var searchChildIndex = func(waitGroup *sync.WaitGroup, in Index, results chan *SearchResult, errs chan error) {
		go func() {
			defer waitGroup.Done()
			searchResult, err := in.Search(req)
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
	ok := true
	for ok {
		select {
		case result, ok = <-results:
			if ok {
				if sr == nil {
					// first result
					sr = result
				} else {
					// merge with previous
					sr.Merge(result)
				}
			}
		case err, ok = <-errs:
		}
	}

	// merge just concatenated all the hits
	// now lets clean it up

	// first sort it by score
	sort.Sort(sr.Hits)

	// now skip over the correct From
	req.From = from
	if req.From > 0 && len(sr.Hits) > req.From {
		sr.Hits = sr.Hits[req.From:]
	} else if req.From > 0 {
		sr.Hits = search.DocumentMatchCollection{}
	}

	// now trim to the correct size
	req.Size = size
	if req.Size > 0 && len(sr.Hits) > req.Size {
		sr.Hits = sr.Hits[0:req.Size]
	}

	// fix up facets
	for name, fr := range req.Facets {
		sr.Facets.Fixup(name, fr.Size)
	}

	// for now stop on any error
	// FIXME offer other behaviors
	if err != nil {
		return nil, err
	}

	return sr, nil
}
