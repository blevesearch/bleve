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

	"github.com/blevesearch/bleve/index/upside_down"
	"github.com/gorilla/mux"
)

// DebugDocumentHandler allows you to debug the index content
// for a given document id.  the document ID should be mapped
// to the mux router URL with name "docId"
type DebugDocumentHandler struct {
	defaultIndexName string
}

func NewDebugDocumentHandler(defaultIndexName string) *DebugDocumentHandler {
	return &DebugDocumentHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DebugDocumentHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

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

	// find the docID
	docID := mux.Vars(req)["docID"]

	rv := make([]interface{}, 0)
	rowChan := index.DumpDoc(docID)
	for row := range rowChan {
		switch row := row.(type) {
		case error:
			showError(w, req, fmt.Sprintf("error debugging document: %v", row), 500)
			return
		case upside_down.UpsideDownCouchRow:
			tmp := struct {
				Key []byte `json:"key"`
				Val []byte `json:"val"`
			}{
				Key: row.Key(),
				Val: row.Value(),
			}
			rv = append(rv, tmp)
		}
	}
	mustEncode(w, rv)
}
