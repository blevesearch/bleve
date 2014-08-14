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
	"fmt"
	"time"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/couchbaselabs/bleve/index/store/leveldb"
	"github.com/couchbaselabs/bleve/index/upside_down"
	"github.com/couchbaselabs/bleve/search"
)

type indexImpl struct {
	s store.KVStore
	i index.Index
	m *IndexMapping
}

func newIndex(path string, mapping *IndexMapping) (*indexImpl, error) {
	// start by validating the index mapping
	err := mapping.Validate()
	if err != nil {
		return nil, err
	}

	store, err := leveldb.Open(path, Config.CreateIfMissing)
	if err != nil {
		return nil, err
	}
	idx := upside_down.NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		return nil, err
	}
	return &indexImpl{
		s: store,
		i: idx,
		m: mapping,
	}, nil
}

func (i *indexImpl) Index(id string, data interface{}) error {
	doc := document.NewDocument(id)
	err := i.m.MapDocument(doc, data)
	if err != nil {
		return err
	}
	err = i.i.Update(doc)
	if err != nil {
		return err
	}
	return nil
}

func (i *indexImpl) Delete(id string) error {
	err := i.i.Delete(id)
	if err != nil {
		return err
	}
	return nil
}

func (i *indexImpl) Batch(b Batch) error {
	ib := make(index.Batch, len(b))
	for bk, bd := range b {
		if bd == nil {
			ib.Delete(bk)
		} else {
			doc := document.NewDocument(bk)
			err := i.m.MapDocument(doc, bd)
			if err != nil {
				return err
			}
			ib.Index(bk, doc)
		}
	}
	return i.i.Batch(ib)
}

func (i *indexImpl) Document(id string) (*document.Document, error) {
	return i.i.Document(id)
}

func (i *indexImpl) DocCount() uint64 {
	return i.i.DocCount()
}

func (i *indexImpl) Search(req *SearchRequest) (*SearchResult, error) {
	collector := search.NewTopScorerSkipCollector(req.Size, req.From)
	searcher, err := req.Query.Searcher(i, req.Explain)
	if err != nil {
		return nil, err
	}

	if req.Facets != nil {
		facetsBuilder := search.NewFacetsBuilder(i.i)
		for facetName, facetRequest := range req.Facets {
			if facetRequest.NumericRanges != nil {
				// build numeric range facet
				facetBuilder := search.NewNumericFacetBuilder(facetRequest.Field, facetRequest.Size)
				for _, nr := range facetRequest.NumericRanges {
					facetBuilder.AddRange(nr.Name, nr.Min, nr.Max)
				}
				facetsBuilder.Add(facetName, facetBuilder)
			} else if facetRequest.DateTimeRanges != nil {
				// build date range facet
				facetBuilder := search.NewDateTimeFacetBuilder(facetRequest.Field, facetRequest.Size)
				dateTimeParser := i.m.DateTimeParserNamed(i.m.DefaultDateTimeParser)
				for _, dr := range facetRequest.DateTimeRanges {
					dr.ParseDates(dateTimeParser)
					facetBuilder.AddRange(dr.Name, dr.Start, dr.End)
				}
				facetsBuilder.Add(facetName, facetBuilder)
			} else {
				// build terms facet
				facetBuilder := search.NewTermsFacetBuilder(facetRequest.Field, facetRequest.Size)
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
		highlighter := Config.Highlight.Highlighters[*Config.DefaultHighlighter]
		if req.Highlight.Style != nil {
			highlighter = Config.Highlight.Highlighters[*req.Highlight.Style]
			if highlighter == nil {
				return nil, fmt.Errorf("no highlighter named `%s` registered", *req.Highlight.Style)
			}
		}

		for _, hit := range hits {
			doc, err := i.Document(hit.ID)
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
					highlighter.BestFragmentsInField(hit, doc, hf, 3)
				}
			}
		}
	}

	if len(req.Fields) > 0 {
		for _, hit := range hits {
			// FIXME avoid loading doc second time
			// if we already loaded it for highlighting
			doc, err := i.Document(hit.ID)
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

func (i *indexImpl) Dump() {
	i.i.Dump()
}

func (i *indexImpl) Fields() ([]string, error) {
	return i.i.Fields()
}

func (i *indexImpl) DumpFields() {
	i.i.DumpFields()
}

func (i *indexImpl) DumpDoc(id string) ([]interface{}, error) {
	return i.i.DumpDoc(id)
}

func (i *indexImpl) Close() {
	i.i.Close()
}
