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

package query

import (
	"context"

	"github.com/blevesearch/bleve/v2/search/searcher"
)

const (
	CustomFilterContextKey = "custom_filter"
	CustomScoreContextKey  = "custom_score"
)

// CustomFilterFactory lets the embedding application provide request-scoped
// filter callbacks created from query-provided source/params/fields.
type CustomFilterFactory func(source string, params map[string]interface{}, fields []string) (searcher.FilterFunc, error)

// CustomScoreFactory lets the embedding application provide request-scoped
// score callbacks created from query-provided source/params/fields.
type CustomScoreFactory func(source string, params map[string]interface{}, fields []string) (searcher.ScoreFunc, error)

// WithCustomFilterFactory returns a context carrying the request-scoped
// filter factory used by CustomFilterQuery during searcher construction.
func WithCustomFilterFactory(ctx context.Context, factory CustomFilterFactory) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, CustomFilterContextKey, factory)
}

// WithCustomScoreFactory returns a context carrying the request-scoped
// score factory used by CustomScoreQuery during searcher construction.
func WithCustomScoreFactory(ctx context.Context, factory CustomScoreFactory) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, CustomScoreContextKey, factory)
}

// WithCustomFilterFunc returns a context carrying a direct per-hit filter
// callback for standalone Bleve use-cases that do not need source/params/fields.
func WithCustomFilterFunc(ctx context.Context, filter searcher.FilterFunc) context.Context {
	return WithCustomFilterFactory(ctx,
		func(source string, params map[string]interface{}, fields []string) (searcher.FilterFunc, error) {
			return filter, nil
		})
}

// WithCustomScoreFunc returns a context carrying a direct per-hit score
// callback for standalone Bleve use-cases that do not need source/params/fields.
func WithCustomScoreFunc(ctx context.Context, score searcher.ScoreFunc) context.Context {
	return WithCustomScoreFactory(ctx,
		func(source string, params map[string]interface{}, fields []string) (searcher.ScoreFunc, error) {
			return score, nil
		})
}

// WithCustomFactories returns a context carrying both request-scoped
// custom query factories for search execution.
func WithCustomFactories(ctx context.Context, filterFactory CustomFilterFactory,
	scoreFactory CustomScoreFactory) context.Context {
	ctx = WithCustomFilterFactory(ctx, filterFactory)
	return WithCustomScoreFactory(ctx, scoreFactory)
}

// WithCustomFuncs returns a context carrying direct per-hit callbacks for
// standalone Bleve use-cases that do not need source/params/fields.
func WithCustomFuncs(ctx context.Context, filter searcher.FilterFunc,
	score searcher.ScoreFunc) context.Context {
	ctx = WithCustomFilterFunc(ctx, filter)
	return WithCustomScoreFunc(ctx, score)
}
