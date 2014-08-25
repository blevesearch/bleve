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
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
)

type DocIndexHandler struct {
	defaultIndexName string
}

func NewDocIndexHandler(defaultIndexName string) *DocIndexHandler {
	return &DocIndexHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DocIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

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

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	err = index.Index(docId, requestBody)
	if err != nil {
		showError(w, req, fmt.Sprintf("error indexing document '%s': %v", docId, err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}
