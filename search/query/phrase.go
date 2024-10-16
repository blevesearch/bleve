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

package query

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

type PhraseQuery struct {
	Terms     []string `json:"terms"`
	FieldVal  string   `json:"field,omitempty"`
	BoostVal  *Boost   `json:"boost,omitempty"`
	Fuzziness int      `json:"fuzziness"`
}

// NewPhraseQuery creates a new Query for finding
// exact term phrases in the index.
// The provided terms must exist in the correct
// order, at the correct index offsets, in the
// specified field. Queried field must have been indexed with
// IncludeTermVectors set to true.
func NewPhraseQuery(terms []string, field string) *PhraseQuery {
	return &PhraseQuery{
		Terms:    terms,
		FieldVal: field,
	}
}

func (q *PhraseQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *PhraseQuery) SetFuzziness(f int) {
	q.Fuzziness = f
}

func (q *PhraseQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *PhraseQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *PhraseQuery) Field() string {
	return q.FieldVal
}

func (q *PhraseQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	return searcher.NewPhraseSearcher(ctx, i, q.Terms, q.Fuzziness, q.FieldVal, q.BoostVal.Value(), options)
}

func (q *PhraseQuery) Validate() error {
	if len(q.Terms) < 1 {
		return fmt.Errorf("phrase query must contain at least one term")
	}
	return nil
}

func (q *PhraseQuery) UnmarshalJSON(data []byte) error {
	type _phraseQuery PhraseQuery
	tmp := _phraseQuery{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}
	q.Terms = tmp.Terms
	q.FieldVal = tmp.FieldVal
	q.BoostVal = tmp.BoostVal
	q.Fuzziness = tmp.Fuzziness
	return nil
}
