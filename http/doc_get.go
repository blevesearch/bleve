//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/blevesearch/bleve/document"

	"github.com/gorilla/mux"
)

type DocGetHandler struct {
	defaultIndexName string
}

func NewDocGetHandler(defaultIndexName string) *DocGetHandler {
	return &DocGetHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DocGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the index to operate on
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	index := IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	// find the doc id
	docId := mux.Vars(req)["docId"]
	if docId == "" {
		showError(w, req, "document id cannot be empty", 400)
		return
	}

	doc, err := index.Document(docId)
	if err != nil {
		showError(w, req, fmt.Sprintf("error deleting document '%s': %v", docId, err), 500)
		return
	}
	if doc == nil {
		showError(w, req, fmt.Sprintf("no such document '%s'", docId), 404)
		return
	}

	rv := struct {
		ID     string                 `json:"id"`
		Fields map[string]interface{} `json:"fields"`
	}{
		ID:     docId,
		Fields: map[string]interface{}{},
	}
	for _, field := range doc.Fields {
		var newval interface{}
		switch field := field.(type) {
		case *document.TextField:
			newval = string(field.Value())
		case *document.NumericField:
			n, err := field.Number()
			if err == nil {
				newval = n
			}
		case *document.DateTimeField:
			d, err := field.DateTime()
			if err == nil {
				newval = d.Format(time.RFC3339Nano)
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
	}

	mustEncode(w, rv)
}
