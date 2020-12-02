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

	"github.com/blevesearch/bleve/index/upsidedown"
)

// DebugDocumentHandler allows you to debug the index content
// for a given document id.
type DebugDocumentHandler struct {
	defaultIndexName string
	IndexNameLookup  varLookupFunc
	DocIDLookup      varLookupFunc
}

func NewDebugDocumentHandler(defaultIndexName string) *DebugDocumentHandler {
	return &DebugDocumentHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DebugDocumentHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	// find the index to operate on
	var indexName string
	if h.IndexNameLookup != nil {
		indexName = h.IndexNameLookup(req)
	}
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	index := IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	// find the docID
	var docID string
	if h.DocIDLookup != nil {
		docID = h.DocIDLookup(req)
	}

	internalIndex, err := index.Advanced()
	if err != nil {
		showError(w, req, fmt.Sprintf("error getting index: %v", err), 500)
		return
	}
	internalIndexReader, err := internalIndex.Reader()
	if err != nil {
		showError(w, req, fmt.Sprintf("error operning index reader: %v", err), 500)
		return
	}

	var rv []interface{}
	rowChan := internalIndexReader.DumpDoc(docID)
	for row := range rowChan {
		switch row := row.(type) {
		case error:
			showError(w, req, fmt.Sprintf("error debugging document: %v", row), 500)
			return
		case upsidedown.UpsideDownCouchRow:
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
	err = internalIndexReader.Close()
	if err != nil {
		showError(w, req, fmt.Sprintf("error closing index reader: %v", err), 500)
		return
	}
	mustEncode(w, rv)
}
