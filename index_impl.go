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
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/upside_down"
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collectors"
	"github.com/blevesearch/bleve/search/facets"
	"github.com/blevesearch/bleve/search/highlight"
)

type indexImpl struct {
	path  string
	name  string
	meta  *indexMeta
	i     index.Index
	m     *IndexMapping
	mutex sync.RWMutex
	open  bool
	stats *IndexStat
}

const storePath = "store"

var mappingInternalKey = []byte("_mapping")

func indexStorePath(path string) string {
	return path + string(os.PathSeparator) + storePath
}

func newMemIndex(indexType string, mapping *IndexMapping) (*indexImpl, error) {
	rv := indexImpl{
		path: "",
		name: "mem",
		m:    mapping,
		meta: newIndexMeta(indexType, gtreap.Name, nil),
	}

	rv.stats = &IndexStat{i: &rv}

	// open the index
	indexTypeConstructor := registry.IndexTypeConstructorByName(rv.meta.IndexType)
	if indexTypeConstructor == nil {
		return nil, ErrorUnknownIndexType
	}

	var err error
	rv.i, err = indexTypeConstructor(rv.meta.Storage, nil, Config.analysisQueue)
	if err != nil {
		return nil, err
	}
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
	indexStats.Register(&rv)
	return &rv, nil
}

func newIndexUsing(path string, mapping *IndexMapping, indexType string, kvstore string, kvconfig map[string]interface{}) (*indexImpl, error) {
	// first validate the mapping
	err := mapping.Validate()
	if err != nil {
		return nil, err
	}

	if path == "" {
		return newMemIndex(indexType, mapping)
	}

	if kvconfig == nil {
		kvconfig = map[string]interface{}{}
	}

	if kvstore == "" {
		return nil, fmt.Errorf("bleve not configured for file based indexing")
	}

	rv := indexImpl{
		path: path,
		name: path,
		m:    mapping,
		meta: newIndexMeta(indexType, kvstore, kvconfig),
	}
	rv.stats = &IndexStat{i: &rv}
	// at this point there is hope that we can be successful, so save index meta
	err = rv.meta.Save(path)
	if err != nil {
		return nil, err
	}
	kvconfig["create_if_missing"] = true
	kvconfig["error_if_exists"] = true
	kvconfig["path"] = indexStorePath(path)

	// open the index
	indexTypeConstructor := registry.IndexTypeConstructorByName(rv.meta.IndexType)
	if indexTypeConstructor == nil {
		return nil, ErrorUnknownIndexType
	}

	rv.i, err = indexTypeConstructor(rv.meta.Storage, kvconfig, Config.analysisQueue)
	if err != nil {
		return nil, err
	}
	err = rv.i.Open()
	if err != nil {
		if err == index.ErrorUnknownStorageType {
			return nil, ErrorUnknownStorageType
		}
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
	indexStats.Register(&rv)
	return &rv, nil
}

func openIndexUsing(path string, runtimeConfig map[string]interface{}) (rv *indexImpl, err error) {
	rv = &indexImpl{
		path: path,
		name: path,
	}
	rv.stats = &IndexStat{i: rv}

	rv.meta, err = openIndexMeta(path)
	if err != nil {
		return nil, err
	}

	// backwards compatibility if index type is missing
	if rv.meta.IndexType == "" {
		rv.meta.IndexType = upside_down.Name
	}

	storeConfig := rv.meta.Config
	if storeConfig == nil {
		storeConfig = map[string]interface{}{}
	}

	storeConfig["path"] = indexStorePath(path)
	storeConfig["create_if_missing"] = false
	storeConfig["error_if_exists"] = false
	for rck, rcv := range runtimeConfig {
		storeConfig[rck] = rcv
	}

	// open the index
	indexTypeConstructor := registry.IndexTypeConstructorByName(rv.meta.IndexType)
	if indexTypeConstructor == nil {
		return nil, ErrorUnknownIndexType
	}

	rv.i, err = indexTypeConstructor(rv.meta.Storage, storeConfig, Config.analysisQueue)
	if err != nil {
		return nil, err
	}
	err = rv.i.Open()
	if err != nil {
		if err == index.ErrorUnknownStorageType {
			return nil, ErrorUnknownStorageType
		}
		return nil, err
	}

	// now load the mapping
	indexReader, err := rv.i.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := indexReader.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	mappingBytes, err := indexReader.GetInternal(mappingInternalKey)
	if err != nil {
		return nil, err
	}

	var im IndexMapping
	err = json.Unmarshal(mappingBytes, &im)
	if err != nil {
		return nil, fmt.Errorf("error parsing mapping JSON: %v\nmapping contents:\n%s", err, string(mappingBytes))
	}

	// mark the index as open
	rv.mutex.Lock()
	defer rv.mutex.Unlock()
	rv.open = true

	// validate the mapping
	err = im.Validate()
	if err != nil {
		// note even if the mapping is invalid
		// we still return an open usable index
		return rv, err
	}

	rv.m = &im
	indexStats.Register(rv)
	return rv, err
}

// Advanced returns implementation internals
// necessary ONLY for advanced usage.
func (i *indexImpl) Advanced() (index.Index, store.KVStore, error) {
	s, err := i.i.Advanced()
	if err != nil {
		return nil, nil, err
	}
	return i.i, s, nil
}

// Mapping returns the IndexMapping in use by this
// Index.
func (i *indexImpl) Mapping() *IndexMapping {
	return i.m
}

// Index the object with the specified identifier.
// The IndexMapping for this index will determine
// how the object is indexed.
func (i *indexImpl) Index(id string, data interface{}) (err error) {
	if id == "" {
		return ErrorEmptyID
	}

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	doc := document.NewDocument(id)
	err = i.m.mapDocument(doc, data)
	if err != nil {
		return
	}
	err = i.i.Update(doc)
	return
}

// Delete entries for the specified identifier from
// the index.
func (i *indexImpl) Delete(id string) (err error) {
	if id == "" {
		return ErrorEmptyID
	}

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err = i.i.Delete(id)
	return
}

// Batch executes multiple Index and Delete
// operations at the same time.  There are often
// significant performance benefits when performing
// operations in a batch.
func (i *indexImpl) Batch(b *Batch) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	return i.i.Batch(b.internal)
}

// Document is used to find the values of all the
// stored fields for a document in the index.  These
// stored fields are put back into a Document object
// and returned.
func (i *indexImpl) Document(id string) (doc *document.Document, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}
	indexReader, err := i.i.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := indexReader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	doc, err = indexReader.Document(id)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// DocCount returns the number of documents in the
// index.
func (i *indexImpl) DocCount() (uint64, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return 0, ErrorIndexClosed
	}

	return i.i.DocCount()
}

// Search executes a search request operation.
// Returns a SearchResult object or an error.
func (i *indexImpl) Search(req *SearchRequest) (sr *SearchResult, err error) {
	return i.SearchInContext(context.Background(), req)
}

// SearchInContext executes a search request operation within the provided
// Context.  Returns a SearchResult object or an error.
func (i *indexImpl) SearchInContext(ctx context.Context, req *SearchRequest) (sr *SearchResult, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	searchStart := time.Now()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	// open a reader for this search
	indexReader, err := i.i.Reader()
	if err != nil {
		return nil, fmt.Errorf("error opening index reader %v", err)
	}
	defer func() {
		if cerr := indexReader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	searcher, err := req.Query.Searcher(indexReader, i.m, req.Explain)
	if err != nil {
		return nil, err
	}
	defer func() {
		if serr := searcher.Close(); err == nil && serr != nil {
			err = serr
		}
	}()

	collector := collectors.NewHeapCollector(req.Size, req.From, indexReader, req.Sort)

	if req.Facets != nil {
		facetsBuilder := search.NewFacetsBuilder(indexReader)
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

	err = collector.Collect(ctx, searcher)
	if err != nil {
		return nil, err
	}

	hits := collector.Results()

	var highlighter highlight.Highlighter

	if req.Highlight != nil {
		// get the right highlighter
		highlighter, err = Config.Cache.HighlighterNamed(Config.DefaultHighlighter)
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
	}

	for _, hit := range hits {
		if len(req.Fields) > 0 || highlighter != nil {
			doc, err := indexReader.Document(hit.ID)
			if err == nil && doc != nil {
				if len(req.Fields) > 0 {
					for _, f := range req.Fields {
						for _, docF := range doc.Fields {
							if f == "*" || docF.Name() == f {
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
								case *document.BooleanField:
									boolean, err := docF.Boolean()
									if err == nil {
										value = boolean
									}
								}
								if value != nil {
									hit.AddFieldValue(docF.Name(), value)
								}
							}
						}
					}
				}
				if highlighter != nil {
					highlightFields := req.Highlight.Fields
					if highlightFields == nil {
						// add all fields with matches
						highlightFields = make([]string, 0, len(hit.Locations))
						for k := range hit.Locations {
							highlightFields = append(highlightFields, k)
						}
					}
					for _, hf := range highlightFields {
						highlighter.BestFragmentsInField(hit, doc, hf, 1)
					}
				}
			} else if doc == nil {
				// unexpected case, a doc ID that was found as a search hit
				// was unable to be found during document lookup
				return nil, ErrorIndexReadInconsistency
			}
		}
		if i.name != "" {
			hit.Index = i.name
		}
	}

	atomic.AddUint64(&i.stats.searches, 1)
	searchDuration := time.Since(searchStart)
	atomic.AddUint64(&i.stats.searchTime, uint64(searchDuration))

	if searchDuration > Config.SlowSearchLogThreshold {
		logger.Printf("slow search took %s - %v", searchDuration, req)
	}

	return &SearchResult{
		Status: &SearchStatus{
			Total:      1,
			Failed:     0,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Request:  req,
		Hits:     hits,
		Total:    collector.Total(),
		MaxScore: collector.MaxScore(),
		Took:     searchDuration,
		Facets:   collector.FacetResults(),
	}, nil
}

// Fields returns the name of all the fields this
// Index has operated on.
func (i *indexImpl) Fields() (fields []string, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	indexReader, err := i.i.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := indexReader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	fields, err = indexReader.Fields()
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func (i *indexImpl) FieldDict(field string) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	indexReader, err := i.i.Reader()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := indexReader.FieldDict(field)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexImplFieldDict{
		index:       i,
		indexReader: indexReader,
		fieldDict:   fieldDict,
	}, nil
}

func (i *indexImpl) FieldDictRange(field string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	indexReader, err := i.i.Reader()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := indexReader.FieldDictRange(field, startTerm, endTerm)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexImplFieldDict{
		index:       i,
		indexReader: indexReader,
		fieldDict:   fieldDict,
	}, nil
}

func (i *indexImpl) FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error) {
	i.mutex.RLock()

	if !i.open {
		i.mutex.RUnlock()
		return nil, ErrorIndexClosed
	}

	indexReader, err := i.i.Reader()
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	fieldDict, err := indexReader.FieldDictPrefix(field, termPrefix)
	if err != nil {
		i.mutex.RUnlock()
		return nil, err
	}

	return &indexImplFieldDict{
		index:       i,
		indexReader: indexReader,
		fieldDict:   fieldDict,
	}, nil
}

// DumpAll writes all index rows to a channel.
// INTERNAL: do not rely on this function, it is
// only intended to be used by the debug utilities
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
// only intended to be used by the debug utilities
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
// only intended to be used by the debug utilities
func (i *indexImpl) DumpDoc(id string) chan interface{} {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil
	}
	return i.i.DumpDoc(id)
}

func (i *indexImpl) Close() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	indexStats.UnRegister(i)

	i.open = false
	return i.i.Close()
}

func (i *indexImpl) Stats() *IndexStat {
	return i.stats
}

func (i *indexImpl) StatsMap() map[string]interface{} {
	return i.stats.statsMap()
}

func (i *indexImpl) GetInternal(key []byte) (val []byte, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	reader, err := i.i.Reader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := reader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	val, err = reader.GetInternal(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (i *indexImpl) SetInternal(key, val []byte) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	return i.i.SetInternal(key, val)
}

func (i *indexImpl) DeleteInternal(key []byte) error {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	return i.i.DeleteInternal(key)
}

// NewBatch creates a new empty batch.
func (i *indexImpl) NewBatch() *Batch {
	return &Batch{
		index:    i,
		internal: index.NewBatch(),
	}
}

func (i *indexImpl) Name() string {
	return i.name
}

func (i *indexImpl) SetName(name string) {
	indexStats.UnRegister(i)
	i.name = name
	indexStats.Register(i)
}

type indexImplFieldDict struct {
	index       *indexImpl
	indexReader index.IndexReader
	fieldDict   index.FieldDict
}

func (f *indexImplFieldDict) Next() (*index.DictEntry, error) {
	return f.fieldDict.Next()
}

func (f *indexImplFieldDict) Close() error {
	defer f.index.mutex.RUnlock()
	err := f.fieldDict.Close()
	if err != nil {
		return err
	}
	return f.indexReader.Close()
}
