//  Copyright (c) 2024 Couchbase, Inc.
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

package bleve

// A preSearchResultProcessor processes the data in
// the preSearch result from multiple
// indexes in an alias and merges them together to
// create the final preSearch result
type preSearchResultProcessor interface {
	// adds the preSearch result to the processor
	add(*SearchResult, string)
	// updates the final search result with the finalized
	// data from the processor
	finalize(*SearchResult)
}

type knnPreSearchResultProcessor struct {
	addFn      func(sr *SearchResult, indexName string)
	finalizeFn func(sr *SearchResult)
}

func (k *knnPreSearchResultProcessor) add(sr *SearchResult, indexName string) {
	if k.addFn != nil {
		k.addFn(sr, indexName)
	}
}

func (k *knnPreSearchResultProcessor) finalize(sr *SearchResult) {
	if k.finalizeFn != nil {
		k.finalizeFn(sr)
	}
}

// -----------------------------------------------------------------------------

func finalizePreSearchResult(req *SearchRequest, preSearchResult *SearchResult) {
	if requestHasKNN(req) {
		preSearchResult.Hits = finalizeKNNResults(req, preSearchResult.Hits)
	}
}

func createPreSearchResultProcessor(req *SearchRequest) preSearchResultProcessor {
	if requestHasKNN(req) {
		return newKnnPreSearchResultProcessor(req)
	}
	return &knnPreSearchResultProcessor{} // equivalent to nil
}
