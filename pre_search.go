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

// A presearchResultProcessor processes the data in
// the presearch result from multiple
// indexes in an alias and merges them together to
// create the final presearch result
type presearchResultProcessor interface {
	// Add adds the presearch result to the processor
	add(*SearchResult, string)
	// Update the final search result with the finalized
	// data from the processor
	finalize(*SearchResult)
}

type knnPresearchResultProcessor struct {
	addFn      func(sr *SearchResult, indexName string)
	finalizeFn func(sr *SearchResult)
}

func (k *knnPresearchResultProcessor) add(sr *SearchResult, indexName string) {
	if k.addFn != nil {
		k.addFn(sr, indexName)
	}
}

func (k *knnPresearchResultProcessor) finalize(sr *SearchResult) {
	if k.finalizeFn != nil {
		k.finalizeFn(sr)
	}
}
