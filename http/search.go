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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/blevesearch/bleve"
)

// SearchHandler can handle search requests
// sent over HTTP the index name can be selected
// in the URL by setting the defaultIndex parameter.
type SearchHandler struct {
	defaultIndexName string
}

func NewSearchHandler(defaultIndexName string) *SearchHandler {
	return &SearchHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	// find the index to operate on
	indexName := req.Form.Get("indexName")
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	index := IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	log.Printf("request body: %s", requestBody)

	// parse the request
	var searchRequest bleve.SearchRequest
	err = json.Unmarshal(requestBody, &searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error parsing query: %v", err), 400)
		return
	}

	log.Printf("parsed request %#v", searchRequest)

	// varlidate the query
	err = searchRequest.Query.Validate()
	if err != nil {
		showError(w, req, fmt.Sprintf("error validating query: %v", err), 400)
		return
	}

	// execute the query
	searchResponse, err := index.Search(&searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error executing query: %v", err), 500)
		return
	}

	// encode the response
	mustEncode(w, searchResponse)
}
