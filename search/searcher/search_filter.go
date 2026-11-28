//  Copyright (c) 2017 Couchbase, Inc.
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

var reflectStaticSizeFilteringSearcher int

func init() {
	var fs FilteringSearcher
	reflectStaticSizeFilteringSearcher = int(reflect.TypeOf(fs).Size())
}

// FilterFunc defines a function which can filter documents
// returning true means keep the document
// returning false means do not keep the document
type FilterFunc func(sctx *search.SearchContext, d *search.DocumentMatch) bool

// FilteringSearcher wraps any other searcher, but checks any Next/Advance
// call against the supplied FilterFunc
type FilteringSearcher struct {
	child  search.Searcher
	accept FilterFunc
}

func NewFilteringSearcher(ctx context.Context, s search.Searcher, filter FilterFunc) *FilteringSearcher {
	return &FilteringSearcher{
		child:  s,
		accept: filter,
	}
}

func (f *FilteringSearcher) Size() int {
	return reflectStaticSizeFilteringSearcher + size.SizeOfPtr +
		f.child.Size()
}

func (f *FilteringSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	next, err := f.child.Next(ctx)
	for next != nil && err == nil {
		if f.accept(ctx, next) {
			return next, nil
		}
		// recycle this document match now, since
		// we do not need it anymore
		ctx.DocumentMatchPool.Put(next)
		next, err = f.child.Next(ctx)
	}
	return nil, err
}

func (f *FilteringSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv == nil {
		return nil, nil
	}
	if f.accept(ctx, adv) {
		return adv, nil
	}
	// recycle this document match now, since
	// we do not need it anymore
	ctx.DocumentMatchPool.Put(adv)
	return f.Next(ctx)
}

func (f *FilteringSearcher) Close() error {
	return f.child.Close()
}

func (f *FilteringSearcher) Weight() float64 {
	return f.child.Weight()
}

func (f *FilteringSearcher) SetQueryNorm(n float64) {
	f.child.SetQueryNorm(n)
}

func (f *FilteringSearcher) Count() uint64 {
	return f.child.Count()
}

func (f *FilteringSearcher) Min() int {
	return f.child.Min()
}

func (f *FilteringSearcher) DocumentMatchPoolSize() int {
	return f.child.DocumentMatchPoolSize()
}

var reflectStaticSizeFilterSearcher int

func init() {
	var fs FilterSearcher
	reflectStaticSizeFilterSearcher = int(reflect.TypeOf(fs).Size())
}

// FilterSearcher keeps only those documents of its child that are also matched
// by filterSearcher. Both are expected to return documents in internal-ID
// order, so filterSearcher is advanced in lockstep with the child and the
// filter does not contribute to the result score.
//
// Unlike FilteringSearcher, which takes a caller-supplied predicate, this
// searcher owns filterSearcher: it is closed together with the child, and any
// error returned while iterating it aborts the search instead of being treated
// as a non-match.
type FilterSearcher struct {
	child          search.Searcher
	filterSearcher search.Searcher
	initialized    bool
	refDoc         *search.DocumentMatch
}

func NewFilterSearcher(ctx context.Context, child, filterSearcher search.Searcher) *FilterSearcher {
	return &FilterSearcher{
		child:          child,
		filterSearcher: filterSearcher,
	}
}

func (f *FilterSearcher) Size() int {
	return reflectStaticSizeFilterSearcher + 2*size.SizeOfPtr +
		f.child.Size() + f.filterSearcher.Size()
}

// matches reports whether d is also matched by filterSearcher, advancing
// filterSearcher as needed. filterSearcher is only ever advanced forward, which
// is valid because the child yields documents in internal-ID order.
func (f *FilterSearcher) matches(ctx *search.SearchContext, d *search.DocumentMatch) (bool, error) {
	if !f.initialized {
		// Initialize the reference document to point
		// to the first document in the filterSearcher
		refDoc, err := f.filterSearcher.Next(ctx)
		if err != nil {
			return false, err
		}
		f.refDoc = refDoc
		f.initialized = true
	}
	if f.refDoc == nil {
		// filterSearcher is exhausted, d is not in filter
		return false, nil
	}
	// Compare document IDs
	cmp := f.refDoc.IndexInternalID.Compare(d.IndexInternalID)
	if cmp < 0 {
		// recycle refDoc now that we do not need it
		ctx.DocumentMatchPool.Put(f.refDoc)
		// filterSearcher is behind the current document, Advance() it
		refDoc, err := f.filterSearcher.Advance(ctx, d.IndexInternalID)
		if err != nil {
			return false, err
		}
		f.refDoc = refDoc
		if f.refDoc == nil {
			return false, nil
		}
		// After advance, check if they're now equal
		cmp = f.refDoc.IndexInternalID.Compare(d.IndexInternalID)
	}
	// cmp >= 0: either equal (match) or filterSearcher is ahead (no match)
	return cmp == 0, nil
}

func (f *FilterSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	next, err := f.child.Next(ctx)
	for next != nil && err == nil {
		keep, ferr := f.matches(ctx, next)
		if ferr != nil {
			return nil, ferr
		}
		if keep {
			return next, nil
		}
		// recycle this document match now, since
		// we do not need it anymore
		ctx.DocumentMatchPool.Put(next)
		next, err = f.child.Next(ctx)
	}
	return nil, err
}

func (f *FilterSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv == nil {
		return nil, nil
	}
	keep, err := f.matches(ctx, adv)
	if err != nil {
		return nil, err
	}
	if keep {
		return adv, nil
	}
	// recycle this document match now, since
	// we do not need it anymore
	ctx.DocumentMatchPool.Put(adv)
	return f.Next(ctx)
}

func (f *FilterSearcher) Close() error {
	err0 := f.child.Close()
	err1 := f.filterSearcher.Close()
	if err0 != nil {
		return err0
	}
	return err1
}

func (f *FilterSearcher) Weight() float64 {
	return f.child.Weight()
}

func (f *FilterSearcher) SetQueryNorm(n float64) {
	f.child.SetQueryNorm(n)
}

func (f *FilterSearcher) Count() uint64 {
	return f.child.Count()
}

func (f *FilterSearcher) Min() int {
	return f.child.Min()
}

func (f *FilterSearcher) DocumentMatchPoolSize() int {
	return f.child.DocumentMatchPoolSize() +
		f.filterSearcher.DocumentMatchPoolSize()
}
