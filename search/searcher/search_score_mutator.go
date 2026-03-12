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

var reflectStaticSizeScoreMutatingSearcher int

func init() {
	var sfs ScoreMutatingSearcher
	reflectStaticSizeScoreMutatingSearcher = int(reflect.TypeOf(sfs).Size())
}

// ScoreFunc defines a function which can mutate document scores
type ScoreFunc func(sctx *search.SearchContext, d *search.DocumentMatch) float64

// ScoreMutatingSearcher wraps any other searcher, but mutates the score
// of any Next/Advance call using the supplied ScoreFunc
type ScoreMutatingSearcher struct {
	child  search.Searcher
	mutate ScoreFunc
}

func NewScoreMutatingSearcher(ctx context.Context, s search.Searcher, mutate ScoreFunc) *ScoreMutatingSearcher {
	return &ScoreMutatingSearcher{
		child:  s,
		mutate: mutate,
	}
}

func (f *ScoreMutatingSearcher) Size() int {
	return reflectStaticSizeScoreMutatingSearcher + size.SizeOfPtr +
		f.child.Size()
}

func (f *ScoreMutatingSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	next, err := f.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	if next != nil {
		next.Score = f.mutate(ctx, next)
	}
	return next, nil
}

func (f *ScoreMutatingSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	adv, err := f.child.Advance(ctx, ID)
	if err != nil {
		return nil, err
	}
	if adv != nil {
		adv.Score = f.mutate(ctx, adv)
	}
	return adv, nil
}

func (f *ScoreMutatingSearcher) Close() error {
	return f.child.Close()
}

func (f *ScoreMutatingSearcher) Weight() float64 {
	return f.child.Weight()
}

func (f *ScoreMutatingSearcher) SetQueryNorm(n float64) {
	f.child.SetQueryNorm(n)
}

func (f *ScoreMutatingSearcher) Count() uint64 {
	return f.child.Count()
}

func (f *ScoreMutatingSearcher) Min() int {
	return f.child.Min()
}

func (f *ScoreMutatingSearcher) DocumentMatchPoolSize() int {
	return f.child.DocumentMatchPoolSize()
}
