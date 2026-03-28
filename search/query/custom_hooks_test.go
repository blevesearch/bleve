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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
)

func TestWithCustomFactories(t *testing.T) {
	filterFactory := CustomFilterFactory(func(source string, params map[string]interface{},
		fields []string) (searcher.FilterFunc, error) {
		return func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
			return true
		}, nil
	})
	scoreFactory := CustomScoreFactory(func(source string, params map[string]interface{},
		fields []string) (searcher.ScoreFunc, error) {
		return func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
			return d.Score
		}, nil
	})

	ctx := WithCustomFactories(nil, filterFactory, scoreFactory)

	if got := customFilterFactoryFromContext(ctx); got == nil {
		t.Fatalf("expected custom filter factory in context")
	}

	if got := customScoreFactoryFromContext(ctx); got == nil {
		t.Fatalf("expected custom score factory in context")
	}

	base := context.Background()
	ctx = WithCustomFilterFactory(base, filterFactory)
	if got := customFilterFactoryFromContext(ctx); got == nil {
		t.Fatalf("expected custom filter factory from WithCustomFilterFactory")
	}
	if got := customScoreFactoryFromContext(ctx); got != nil {
		t.Fatalf("expected no custom score factory in context")
	}
}
