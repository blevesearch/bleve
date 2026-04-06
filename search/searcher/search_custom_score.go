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

// ScoreFunc defines a function which can mutate document scores
type ScoreFunc func(sctx *search.SearchContext, d *search.DocumentMatch) float64

// CustomScoreSearcher wraps any other searcher, optionally loads doc values
// into each DocumentMatch, then mutates the score using the supplied ScoreFunc.
type CustomScoreSearcher struct {
	child       search.Searcher
	mutate      ScoreFunc
	dvReader    index.DocValueReader
	indexReader index.IndexReader
	fieldTypes  map[string]string
}

func NewCustomScoreSearcher(ctx context.Context, s search.Searcher, mutate ScoreFunc,
	dvReader index.DocValueReader, indexReader index.IndexReader,
	fieldTypes map[string]string) *CustomScoreSearcher {
	return &CustomScoreSearcher{
		child:       s,
		mutate:      mutate,
		dvReader:    dvReader,
		indexReader: indexReader,
		fieldTypes:  fieldTypes,
	}
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
		if err = loadDocValuesOnHitWithTypes(next, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
			return nil, err
		}
		next.Score = f.mutate(ctx, next)
	}
	return next, nil
}

func (f *CustomScoreSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv != nil {
		if err = loadDocValuesOnHitWithTypes(adv, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
			return nil, err
		}
		adv.Score = f.mutate(ctx, adv)
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
