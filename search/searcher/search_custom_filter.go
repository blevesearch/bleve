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

// CustomFilterFunc decides whether a hit (with doc-value fields populated)
// should be kept. It receives the search's context so a long-running callback
// (e.g. a JS UDF) can honor the query deadline/cancellation. A non-nil error
// aborts the search so the failure can be surfaced to the caller rather than
// silently dropping the hit.
type CustomFilterFunc func(ctx context.Context, d *search.DocumentMatch) (bool, error)

// CustomFilterSearcher wraps a child searcher, optionally loads doc values
// into each DocumentMatch, then applies a CustomFilterFunc to decide whether
// to keep the hit. Unlike FilteringSearcher this variant is purpose-built for
// custom queries that need field values at callback time.
type CustomFilterSearcher struct {
	ctx         context.Context
	child       search.Searcher
	accept      CustomFilterFunc
	dvReader    index.DocValueReader
	indexReader index.IndexReader
	fieldTypes  map[string]string
}

func NewCustomFilterSearcher(ctx context.Context, child search.Searcher,
	filter CustomFilterFunc, dvReader index.DocValueReader,
	indexReader index.IndexReader,
	fieldTypes map[string]string) *CustomFilterSearcher {
	return &CustomFilterSearcher{
		ctx:         ctx,
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
		// The fields loaded below are UDF input only; remember the hit's prior
		// fields so we can restore them and avoid leaking the UDF's internal
		// fields into (or overriding) SearchRequest.Fields in the response.
		priorFields := next.Fields
		if err = loadDocValuesOnHitWithTypes(next, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
			return nil, err
		}
		keep, ferr := f.accept(f.ctx, next)
		next.Fields = priorFields
		if ferr != nil {
			return nil, ferr
		}
		if keep {
			return next, nil
		}
		ctx.DocumentMatchPool.Put(next)
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
	// See Next: restore the hit's fields after the callback so UDF-input
	// fields don't override SearchRequest.Fields in the response.
	priorFields := adv.Fields
	if err = loadDocValuesOnHitWithTypes(adv, f.dvReader, f.indexReader, f.fieldTypes); err != nil {
		return nil, err
	}
	keep, ferr := f.accept(f.ctx, adv)
	adv.Fields = priorFields
	if ferr != nil {
		return nil, ferr
	}
	if keep {
		return adv, nil
	}
	ctx.DocumentMatchPool.Put(adv)
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
