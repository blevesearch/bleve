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

var reflectStaticSizeCustomFilterSearcher int

func init() {
	var cfs CustomFilterSearcher
	reflectStaticSizeCustomFilterSearcher = int(reflect.TypeOf(cfs).Size())
}

// CustomFilterSearcher wraps a child searcher, optionally loads doc values
// into each DocumentMatch, then applies a FilterFunc to decide whether to keep
// the hit. Unlike FilteringSearcher this variant is purpose-built for custom
// queries that need field values at callback time.
type CustomFilterSearcher struct {
	child       search.Searcher
	accept      FilterFunc
	dvReader    index.DocValueReader
	indexReader index.IndexReader
	fieldTypes  map[string]string
}

func NewCustomFilterSearcher(ctx context.Context, child search.Searcher,
	filter FilterFunc, dvReader index.DocValueReader,
	indexReader index.IndexReader,
	fieldTypes map[string]string) *CustomFilterSearcher {
	return &CustomFilterSearcher{
		child:       child,
		accept:      filter,
		dvReader:    dvReader,
		indexReader: indexReader,
		fieldTypes:  fieldTypes,
	}
}

func (f *CustomFilterSearcher) Size() int {
	return reflectStaticSizeCustomFilterSearcher + size.SizeOfPtr +
		f.child.Size()
}

func (f *CustomFilterSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	next, err := f.child.Next(ctx)
	for next != nil && err == nil {
		if err = loadDocValuesOnHitWithTypes(next, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
			return nil, err
		}
		if f.accept(ctx, next) {
			return next, nil
		}
		next, err = f.child.Next(ctx)
	}
	return nil, err
}

func (f *CustomFilterSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv == nil {
		return nil, nil
	}
	if err = loadDocValuesOnHitWithTypes(adv, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
		return nil, err
	}
	if f.accept(ctx, adv) {
		return adv, nil
	}
	return f.Next(ctx)
}

func (f *CustomFilterSearcher) Close() error {
	return f.child.Close()
}

func (f *CustomFilterSearcher) Weight() float64 {
	return f.child.Weight()
}

func (f *CustomFilterSearcher) SetQueryNorm(n float64) {
	f.child.SetQueryNorm(n)
}

func (f *CustomFilterSearcher) Count() uint64 {
	return f.child.Count()
}

func (f *CustomFilterSearcher) Min() int {
	return f.child.Min()
}

func (f *CustomFilterSearcher) DocumentMatchPoolSize() int {
	return f.child.DocumentMatchPoolSize()
}
