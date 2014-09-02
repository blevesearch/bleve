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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/upside_down"
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collectors"
	"github.com/blevesearch/bleve/search/facets"
)

type indexImpl struct {
	path  string
	meta  *indexMeta
	s     store.KVStore
	i     index.Index
	m     *IndexMapping
	mutex sync.RWMutex
	open  bool
}

const storePath = "store"

var mappingInternalKey = []byte("_mapping")

func indexStorePath(path string) string {
	return path + string(os.PathSeparator) + storePath
}

func newMemIndex(mapping *IndexMapping) (*indexImpl, error) {
	rv := indexImpl{
		path: "",
		m:    mapping,
		meta: newIndexMeta("mem"),
	}

	storeConstructor := registry.KVStoreConstructorByName(rv.meta.Storage)
	if storeConstructor == nil {
		return nil, ErrorUnknownStorageType
	}
	// now open the store
	var err error
	rv.s, err = storeConstructor(nil)
	if err != nil {
		return nil, err
	}

	// open open the index
	rv.i = upside_down.NewUpsideDownCouch(rv.s)
	err = rv.i.Open()
	if err != nil {
		return nil, err
	}

	// now persist the mapping
	mappingBytes, err := json.Marshal(mapping)
	if err != nil {
		return nil, err
	}
	err = rv.i.SetInternal(mappingInternalKey, mappingBytes)
	if err != nil {
		return nil, err
	}

	// mark the index as open
	rv.mutex.Lock()
	defer rv.mutex.Unlock()
	rv.open = true
	return &rv, nil
}

func newIndex(path string, mapping *IndexMapping) (*indexImpl, error) {
	// first validate the mapping
	err := mapping.validate()
	if err != nil {
		return nil, err
	}

	if path == "" {
		return newMemIndex(mapping)
	}

	rv := indexImpl{
		path: path,
		m:    mapping,
		meta: newIndexMeta(Config.DefaultKVStore),
	}
	storeConstructor := registry.KVStoreConstructorByName(rv.meta.Storage)
	if storeConstructor == nil {
		return nil, ErrorUnknownStorageType
	}
	// at this point there hope we can be successful, so save index meta
	err = rv.meta.Save(path)
	if err != nil {
		return nil, err
	}
	storeConfig := map[string]interface{}{
		"path":              indexStorePath(path),
		"create_if_missing": true,
		"error_if_exists":   true,
	}

	// now open the store
	rv.s, err = storeConstructor(storeConfig)
	if err != nil {
		return nil, err
	}

	// open open the index
	rv.i = upside_down.NewUpsideDownCouch(rv.s)
	err = rv.i.Open()
	if err != nil {
		return nil, err
	}

	// now persist the mapping
	mappingBytes, err := json.Marshal(mapping)
	if err != nil {
		return nil, err
	}
	err = rv.i.SetInternal(mappingInternalKey, mappingBytes)
	if err != nil {
		return nil, err
	}

	// mark the index as open
	rv.mutex.Lock()
	defer rv.mutex.Unlock()
	rv.open = true
	return &rv, nil
}

func openIndex(path string) (*indexImpl, error) {

	rv := indexImpl{
		path: path,
	}
	var err error
	rv.meta, err = openIndexMeta(path)
	if err != nil {
		return nil, err
	}

	storeConstructor := registry.KVStoreConstructorByName(rv.meta.Storage)
	if storeConstructor == nil {
		return nil, ErrorUnknownStorageType
	}

	storeConfig := map[string]interface{}{
		"path":              indexStorePath(path),
		"create_if_missing": false,
		"error_if_exists":   false,
	}

	// now open the store
	rv.s, err = storeConstructor(storeConfig)
	if err != nil {
		return nil, err
	}

	// open open the index
	rv.i = upside_down.NewUpsideDownCouch(rv.s)
	err = rv.i.Open()
	if err != nil {
		return nil, err
	}

	// now load the mapping
	mappingBytes, err := rv.i.GetInternal(mappingInternalKey)
	if err != nil {
		return nil, err
	}

	var im IndexMapping
	err = json.Unmarshal(mappingBytes, &im)
	if err != nil {
		return nil, err
	}

	// mark the index as open
	rv.mutex.Lock()
	defer rv.mutex.Unlock()
	rv.open = true

	// validate the mapping
	err = im.validate()
	if err != nil {
		// note even if the mapping is invalid
		// we still return an open usable index
		return &rv, err
	}

	rv.m = &im
	return &rv, nil
}

// Mapping returns the IndexMapping in use by this
// Index.
func (i *indexImpl) Mapping() *IndexMapping {
	return i.m
}

// Index the object with the specified identifier.
// The IndexMapping for this index will determine
// how the object is indexed.
func (i *indexImpl) Index(id string, data interface{}) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if !i.open {
		return ErrorIndexClosed
	}

	doc := document.NewDocument(id)
	err := i.m.mapDocument(doc, data)
	if err != nil {
		return err
	}
	err = i.i.Update(doc)
	if err != nil {
		return err
	}
	return nil
}

// Delete entries for the specified identifier from
// the index.
func (i *indexImpl) Delete(id string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err := i.i.Delete(id)
	if err != nil {
		return err
	}
	return nil
}

// Batch executes multiple Index and Delete
// operations at the same time.  There are often
// significant performance benefits when performing
// operations in a batch.
func (i *indexImpl) Batch(b Batch) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if !i.open {
		return ErrorIndexClosed
	}

	ib := make(index.Batch, len(b))
	for bk, bd := range b {
		if bd == nil {
			ib.Delete(bk)
		} else {
			doc := document.NewDocument(bk)
			err := i.m.mapDocument(doc, bd)
			if err != nil {
				return err
			}
			ib.Index(bk, doc)
		}
	}
	return i.i.Batch(ib)
}

// Document is used to find the values of all the
// stored fields for a document in the index.  These
// stored fields are put back into a Document object
// and returned.
func (i *indexImpl) Document(id string) (*document.Document, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}
	return i.i.Document(id)
}

// DocCount returns the number of documents in the
// index.
func (i *indexImpl) DocCount() uint64 {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return 0
	}

	return i.i.DocCount()
}

// Search executes a search request operation.
// Returns a SearchResult object or an error.
func (i *indexImpl) Search(req *SearchRequest) (*SearchResult, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	collector := collectors.NewTopScorerSkipCollector(req.Size, req.From)
	searcher, err := req.Query.Searcher(i, req.Explain)
	if err != nil {
		return nil, err
	}
	defer searcher.Close()

	if req.Facets != nil {
		facetsBuilder := search.NewFacetsBuilder(i.i)
		for facetName, facetRequest := range req.Facets {
			if facetRequest.NumericRanges != nil {
				// build numeric range facet
				facetBuilder := facets.NewNumericFacetBuilder(facetRequest.Field, facetRequest.Size)
				for _, nr := range facetRequest.NumericRanges {
					facetBuilder.AddRange(nr.Name, nr.Min, nr.Max)
				}
				facetsBuilder.Add(facetName, facetBuilder)
			} else if facetRequest.DateTimeRanges != nil {
				// build date range facet
				facetBuilder := facets.NewDateTimeFacetBuilder(facetRequest.Field, facetRequest.Size)
				dateTimeParser := i.m.dateTimeParserNamed(i.m.DefaultDateTimeParser)
				for _, dr := range facetRequest.DateTimeRanges {
					dr.ParseDates(dateTimeParser)
					facetBuilder.AddRange(dr.Name, dr.Start, dr.End)
				}
				facetsBuilder.Add(facetName, facetBuilder)
			} else {
				// build terms facet
				facetBuilder := facets.NewTermsFacetBuilder(facetRequest.Field, facetRequest.Size)
				facetsBuilder.Add(facetName, facetBuilder)
			}
		}
		collector.SetFacetsBuilder(facetsBuilder)
	}

	err = collector.Collect(searcher)
	if err != nil {
		return nil, err
	}

	hits := collector.Results()

	if req.Highlight != nil {
		// get the right highlighter
		highlighter, err := Config.Cache.HighlighterNamed(Config.DefaultHighlighter)
		if err != nil {
			return nil, err
		}
		if req.Highlight.Style != nil {
			highlighter, err = Config.Cache.HighlighterNamed(*req.Highlight.Style)
			if err != nil {
				return nil, err
			}
		}
		if highlighter == nil {
			return nil, fmt.Errorf("no highlighter named `%s` registered", *req.Highlight.Style)
		}

		for _, hit := range hits {
			doc, err := i.i.Document(hit.ID)
			if err == nil {
				highlightFields := req.Highlight.Fields
				if highlightFields == nil {
					// add all fields with matches
					highlightFields = make([]string, 0, len(hit.Locations))
					for k, _ := range hit.Locations {
						highlightFields = append(highlightFields, k)
					}
				}

				for _, hf := range highlightFields {
					highlighter.BestFragmentsInField(hit, doc, hf, 1)
				}
			}
		}
	}

	if len(req.Fields) > 0 {
		for _, hit := range hits {
			// FIXME avoid loading doc second time
			// if we already loaded it for highlighting
			doc, err := i.i.Document(hit.ID)
			if err == nil {
				for _, f := range req.Fields {
					for _, docF := range doc.Fields {
						if docF.Name() == f {
							var value interface{}
							switch docF := docF.(type) {
							case *document.TextField:
								value = string(docF.Value())
							case *document.NumericField:
								num, err := docF.Number()
								if err == nil {
									value = num
								}
							case *document.DateTimeField:
								datetime, err := docF.DateTime()
								if err == nil {
									value = datetime.Format(time.RFC3339)
								}
							}
							if value != nil {
								hit.AddFieldValue(f, value)
							}
						}
					}
				}
			}
		}
	}

	return &SearchResult{
		Request:  req,
		Hits:     hits,
		Total:    collector.Total(),
		MaxScore: collector.MaxScore(),
		Took:     collector.Took(),
		Facets:   collector.FacetResults(),
	}, nil
}

// Fields returns the name of all the fields this
// Index has operated on.
func (i *indexImpl) Fields() ([]string, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}
	return i.i.Fields()
}

// DumpAll writes all index rows to a channel.
// INTERNAL: do not rely on this function, it is
// only intended to be used by the debug utilties
func (i *indexImpl) DumpAll() chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}

	return i.i.DumpAll()
}

// DumpFields writes all field rows in the index
// to a channel.
// INTERNAL: do not rely on this function, it is
// only intended to be used by the debug utilties
func (i *indexImpl) DumpFields() chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}
	return i.i.DumpFields()
}

// DumpDoc writes all rows in the index associated
// with the specified identifier to a channel.
// INTERNAL: do not rely on this function, it is
// only intended to be used by the debug utilties
func (i *indexImpl) DumpDoc(id string) chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}
	return i.i.DumpDoc(id)
}

func (i *indexImpl) Close() {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.open = false
	i.i.Close()
}
