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

package http

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/microseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/milliseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/nanoseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/seconds"
	index "github.com/blevesearch/bleve_index_api"
)

type DocGetHandler struct {
	defaultIndexName string
	IndexNameLookup  varLookupFunc
	DocIDLookup      varLookupFunc
}

func NewDocGetHandler(defaultIndexName string) *DocGetHandler {
	return &DocGetHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DocGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the index to operate on
	var indexName string
	if h.IndexNameLookup != nil {
		indexName = h.IndexNameLookup(req)
	}
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	idx := IndexByName(indexName)
	if idx == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	// find the doc id
	var docID string
	if h.DocIDLookup != nil {
		docID = h.DocIDLookup(req)
	}
	if docID == "" {
		showError(w, req, "document id cannot be empty", 400)
		return
	}

	doc, err := idx.Document(docID)
	if err != nil {
		showError(w, req, fmt.Sprintf("error deleting document '%s': %v", docID, err), 500)
		return
	}
	if doc == nil {
		showError(w, req, fmt.Sprintf("no such document '%s'", docID), 404)
		return
	}

	rv := struct {
		ID     string                 `json:"id"`
		Fields map[string]interface{} `json:"fields"`
	}{
		ID:     docID,
		Fields: map[string]interface{}{},
	}

	doc.VisitFields(func(field index.Field) {
		var newval interface{}
		switch field := field.(type) {
		case index.TextField:
			newval = field.Text()
		case index.NumericField:
			n, err := field.Number()
			if err == nil {
				newval = n
			}
		case index.DateTimeField:
			d, layout, err := field.DateTime()
			if err == nil {
				if layout == "" {
					// missing layout means we fallback to
					// the default layout which is RFC3339
					newval = d.Format(time.RFC3339)
				} else {
					// the layout here can now either be representative
					// of an actual layout or a timestamp
					switch layout {
					case seconds.Name:
						newval = strconv.FormatInt(d.Unix(), 10)
					case milliseconds.Name:
						newval = strconv.FormatInt(d.UnixMilli(), 10)
					case microseconds.Name:
						newval = strconv.FormatInt(d.UnixMicro(), 10)
					case nanoseconds.Name:
						newval = strconv.FormatInt(d.UnixNano(), 10)
					default:
						newval = d.Format(layout)
					}
				}
			}
		}
		existing, existed := rv.Fields[field.Name()]
		if existed {
			switch existing := existing.(type) {
			case []interface{}:
				rv.Fields[field.Name()] = append(existing, newval)
			case interface{}:
				arr := make([]interface{}, 2)
				arr[0] = existing
				arr[1] = newval
				rv.Fields[field.Name()] = arr
			}
		} else {
			rv.Fields[field.Name()] = newval
		}
	})

	mustEncode(w, rv)
}
