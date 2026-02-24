# Custom Query Hooks: `custom_filter` and `custom_score`

Bleve supports two query nodes for embedding-defined per-hit logic:

- `custom_filter`: keep/drop each hit.
- `custom_score`: override each hit score.

Bleve treats `source` as an opaque payload. The embedding application provides execution via request-scoped callbacks in `context.Context`.

## Query Shape

### `custom_filter`

```json
{
  "query": {
    "custom_filter": {
      "query": { "match": "beer" },
      "source": "my_filter_source",
      "params": { "min": 10.0 },
      "fields": ["abv", "type"]
    }
  }
}
```

### `custom_score`

```json
{
  "query": {
    "custom_score": {
      "query": { "match": "beer" },
      "source": "my_score_source",
      "params": { "mult": 0.05 },
      "fields": ["abv"]
    }
  }
}
```

## Integrating From an Embedding Application

Register callback builders in request context before calling `SearchInContext`.

```go
package main

import (
	"context"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	bleveQuery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
)

func withCustomHooks(ctx context.Context) context.Context {
	filterBuilder := bleveQuery.CustomFilterFactory(
		func(source string, params map[string]interface{}, fields []string) (searcher.FilterFunc, error) {
			// Build a per-hit filter callback from source/params/fields.
			return func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
				// Evaluate hit here; source interpretation is embedder-defined.
				return true
			}, nil
		})

	scoreBuilder := bleveQuery.CustomScoreFactory(
		func(source string, params map[string]interface{}, fields []string) (searcher.ScoreFunc, error) {
			// Build a per-hit score callback from source/params/fields.
			return func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
				// Evaluate hit here; source interpretation is embedder-defined.
				return d.Score
			}, nil
		})

	ctx = context.WithValue(ctx, bleveQuery.CustomFilterContextKey, filterBuilder)
	ctx = context.WithValue(ctx, bleveQuery.CustomScoreContextKey, scoreBuilder)
	return ctx
}

func run(index bleve.Index, req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	ctx := withCustomHooks(context.Background())
	return index.SearchInContext(ctx, req)
}
```

## Runtime Behavior

- Bleve builds the inner query searcher first.
- On `custom_filter`/`custom_score`, Bleve retrieves the corresponding callback builder from context.
- Bleve wraps the child searcher:
  - `custom_filter` -> `FilteringSearcher`
  - `custom_score` -> `ScoreMutatingSearcher`
- Callback runs per hit during `Next()`/`Advance()`.

## Error Cases

- Missing callback builder in context:
  - `custom_filter`: `no custom filter factory registered in context`
  - `custom_score`: `no custom score factory registered in context`
- If builder returns an error, searcher construction fails for that query branch.
