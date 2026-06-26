//  Copyright (c) 2026 Couchbase, Inc.
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
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeCustomScoreSearcher int

func init() {
	var sfs CustomScoreSearcher
	reflectStaticSizeCustomScoreSearcher = int(reflect.TypeOf(sfs).Size())
}

// CustomScoreFunc defines a function which can mutate document scores. It
// receives the search's context so a long-running callback (e.g. a JS UDF) can
// honor the query deadline/cancellation. A non-nil error aborts the search so
// the failure can be surfaced to the caller rather than silently falling back
// to the original score.
type CustomScoreFunc func(ctx context.Context, d *search.DocumentMatch) (float64, error)

// CustomScoreSearcher wraps any other searcher, optionally loads doc values
// into each DocumentMatch, then mutates the score using the supplied
// CustomScoreFunc.
type CustomScoreSearcher struct {
	ctx         context.Context
	child       search.Searcher
	mutate      CustomScoreFunc
	dvReader    index.DocValueReader
	indexReader index.IndexReader
	fieldTypes  map[string]string
	explain     bool
}

func NewCustomScoreSearcher(ctx context.Context, s search.Searcher, mutate CustomScoreFunc,
	dvReader index.DocValueReader, indexReader index.IndexReader,
	fieldTypes map[string]string, explain bool) *CustomScoreSearcher {
	return &CustomScoreSearcher{
		ctx:         ctx,
		child:       s,
		mutate:      mutate,
		dvReader:    dvReader,
		indexReader: indexReader,
		fieldTypes:  fieldTypes,
		explain:     explain,
	}
}

// applyScore mutates the score on the hit and, when explain is enabled,
// replaces the explanation with a single node describing the custom score
// result. A non-nil error from the score function is returned so the caller
// can abort the search.
func (f *CustomScoreSearcher) applyScore(d *search.DocumentMatch) error {
	score, err := f.mutate(f.ctx, d)
	if err != nil {
		return err
	}
	d.Score = score
	if f.explain {
		d.Expl = &search.Explanation{
			Value:   d.Score,
			Message: "custom_score function result",
		}
	}
	return nil
}

func (f *CustomScoreSearcher) Size() int {
	return reflectStaticSizeCustomScoreSearcher + size.SizeOfPtr +
		f.child.Size()
}

func (f *CustomScoreSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	next, err := f.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	if next != nil {
		// Put the loaded fields on the hit only for scoring, so UDF-input fields
		// don't override SearchRequest.Fields in the response.
		udfFields, lerr := loadDocValuesOnHitWithTypes(next, f.dvReader, f.indexReader, f.fieldTypes)
		if lerr != nil {
			return nil, lerr
		}
		priorFields := next.Fields
		next.Fields = udfFields
		serr := f.applyScore(next)
		next.Fields = priorFields
		if serr != nil {
			return nil, serr
		}
	}
	return next, nil
}

func (f *CustomScoreSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv != nil {
		// See Next: put the loaded fields on the hit only for scoring.
		udfFields, lerr := loadDocValuesOnHitWithTypes(adv, f.dvReader, f.indexReader, f.fieldTypes)
		if lerr != nil {
			return nil, lerr
		}
		priorFields := adv.Fields
		adv.Fields = udfFields
		serr := f.applyScore(adv)
		adv.Fields = priorFields
		if serr != nil {
			return nil, serr
		}
	}
	return adv, nil
}

func (f *CustomScoreSearcher) Close() error {
	return f.child.Close()
}

func (f *CustomScoreSearcher) Weight() float64 {
	return f.child.Weight()
}

func (f *CustomScoreSearcher) SetQueryNorm(n float64) {
	f.child.SetQueryNorm(n)
}

func (f *CustomScoreSearcher) Count() uint64 {
	return f.child.Count()
}

func (f *CustomScoreSearcher) Min() int {
	return f.child.Min()
}

func (f *CustomScoreSearcher) DocumentMatchPoolSize() int {
	return f.child.DocumentMatchPoolSize()
}
