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

package searcher

import (
	"context"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// NewFieldExistsSearcher creates a searcher that matches all documents
// containing the specified field, regardless of the field's value.
func NewFieldExistsSearcher(ctx context.Context, indexReader index.IndexReader,
	field string, boost float64, options search.SearcherOptions) (
	search.Searcher, error) {

	// Get all terms in the field using FieldDict
	fieldDict, err := indexReader.FieldDict(field)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := fieldDict.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// Collect all terms in this field
	var terms []string
	tfd, err := fieldDict.Next()
	for err == nil && tfd != nil {
		terms = append(terms, tfd.Term)
		if tooManyClauses(len(terms)) {
			return nil, tooManyClausesErr(field, len(terms))
		}
		tfd, err = fieldDict.Next()
	}
	if err != nil {
		return nil, err
	}

	if len(terms) < 1 {
		return NewMatchNoneSearcher(indexReader)
	}

	if ctx != nil {
		reportIOStats(ctx, fieldDict.BytesRead())
		search.RecordSearchCost(ctx, search.AddM, fieldDict.BytesRead())
	}

	// Create a multi-term searcher that matches any document with any term in this field
	return NewMultiTermSearcher(ctx, indexReader, terms, field, boost, options, true)
}
