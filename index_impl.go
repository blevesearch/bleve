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
	store, err := leveldb.Open(path)
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

// Index the provided data.
func (i *indexImpl) Index(data interface{}) error {
	id, ok := i.determineID(data)
	if ok {
		return i.IndexID(id, data)
	}

	return ERROR_NO_ID
}

func (i *indexImpl) IndexID(id string, data interface{}) error {
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

func (i *indexImpl) IndexJSON(data []byte) error {
	var obj interface{}
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return err
	}
	return i.Index(obj)
}

func (i *indexImpl) IndexJSONID(id string, data []byte) error {
	var obj interface{}
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return err
	}
	return i.IndexID(id, obj)
}

func (i *indexImpl) Delete(data interface{}) error {
	id, ok := i.determineID(data)
	if ok {
		return i.DeleteID(id)
	}

	return ERROR_NO_ID
}

func (i *indexImpl) DeleteID(id string) error {
	err := i.i.Delete(id)
	if err != nil {
		return err
	}
	return nil
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
	err = collector.Collect(searcher)
	if err != nil {
		return nil, err
	}

	hits := collector.Results()

	if req.Highlight != nil {
		// get the right highlighter
		highlighter := config.Highlight.Highlighters[*config.DefaultHighlighter]
		if req.Highlight.Style != nil {
			highlighter = config.Highlight.Highlighters[*req.Highlight.Style]
			if highlighter == nil {
				return nil, fmt.Errorf("no highlighter named `%s` registered", req.Highlight.Style)
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

	return &SearchResult{
		Request:  req,
		Hits:     hits,
		Total:    collector.Total(),
		MaxScore: collector.MaxScore(),
		Took:     collector.Took(),
	}, nil
}

func (i *indexImpl) DumpDoc(id string) ([]interface{}, error) {
	return i.i.DumpDoc(id)
}

func (i *indexImpl) Close() {
	i.i.Close()
}

func (i *indexImpl) determineID(data interface{}) (string, bool) {
	// first see if the object implements Identifier
	identifier, ok := data.(Identifier)
	if ok {
		return identifier.ID(), true
	}

	// now see if we can find an ID using the mapping
	if i.m.IdField != nil {
		id, ok := mustString(lookupPropertyPath(data, *i.m.IdField))
		if ok {
			return id, true
		}
	}

	return "", false
}
