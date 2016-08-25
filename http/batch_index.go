//  Copyright (c) 2016 Couchbase, Inc.
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
	"github.com/blevesearch/bleve"
	"io"
	"net/http"
)

type MetaData struct {
	Action string `json:"action"`
	Index  string `json:"index"`
	Id     string `json:"id"`
}

type BatchIndexHandler struct {
}

func NewBatchIndexHandler() *BatchIndexHandler {
	return &BatchIndexHandler{}
}

func (h *BatchIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	openBatches := map[string]*bleve.Batch{}

	dec := json.NewDecoder(req.Body)
	isMeta := true
	var currentMeta MetaData

	for {
		if isMeta {
			var meta MetaData
			if err := dec.Decode(&meta); err == io.EOF {
				break
			} else if err != nil {
				showError(w, req, fmt.Sprintf("error parsing meta JSON: %v", err), 400)
				return
			}
			currentMeta = meta

			batch, present := openBatches[currentMeta.Index]

			if !present {
				index := IndexByName(currentMeta.Index)
				if index == nil {
					showError(w, req, fmt.Sprintf("no such  index '%s'", currentMeta.Index), 404)
					return
				}
				batch = index.NewBatch()
				openBatches[currentMeta.Index] = batch
			}

			switch currentMeta.Action {
			case "index":
				isMeta = false
			case "delete":
				batch.Delete(currentMeta.Id)
			default:
				showError(w, req, fmt.Sprintf("unknown action: %v", currentMeta.Action), 400)
				return
			}

		} else {
			var doc interface{}
			if err := dec.Decode(&doc); err == io.EOF {
				break
			} else if err != nil {
				showError(w, req, fmt.Sprintf("error parsing document JSON: %v", err), 400)
				return
			}

			batch, _ := openBatches[currentMeta.Index]

			switch currentMeta.Action {
			case "index":
				err := batch.Index(currentMeta.Id, doc)
				if err != nil {
					showError(w, req, fmt.Sprintf("error indexing document in batch '%s': %v", currentMeta.Id, err), 500)
					return
				}
			default:
				showError(w, req, fmt.Sprintf("unknown action: %v", currentMeta.Action), 400)
				return
			}
			isMeta = true
		}

	}

	for idx, batch := range openBatches {
		index := IndexByName(idx)
		err := index.Batch(batch)
		if err != nil {
			showError(w, req, fmt.Sprintf("error while storing batch: %v", err), 400)
			return
		}
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}
