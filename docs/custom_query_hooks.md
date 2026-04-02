# Custom Query Hooks: `custom_filter` and `custom_score`

Bleve exposes two query nodes for embedding-defined per-hit logic:

- `custom_filter`: keep or drop each candidate hit.
- `custom_score`: mutate each candidate hit score.

Bleve itself only executes callbacks that were already provided/attached by the
embedding application. It does not interpret any
embedder-specific payload such as callback source, params, or requested fields.

## Query Objects

`CustomFilterQuery` contains:

- a child query
- a `searcher.FilterFunc` callback attached by the embedder

`CustomScoreQuery` contains:

- a child query
- a `searcher.ScoreFunc` callback attached by the embedder

## Constructors

Embedding applications can construct the nodes directly with callbacks:

```go
filterQuery := query.NewCustomFilterQueryWithFilter(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
		return true
	},
)

scoreQuery := query.NewCustomScoreQueryWithScorer(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
		return d.Score
	},
)
```

These constructors also accept an optional payload map as the third argument:

```go
payload := map[string]interface{}{
	"fields": []string{"abv", "ibu"},
	"params": map[string]interface{}{"weight": 0.05},
	"source": "function score(doc, params) { ... }",
}

scoreQuery := query.NewCustomScoreQueryWithScorer(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
		return d.Score
	},
	payload, // optional; nil is valid
)
```

Payload rules:

- pass only custom-node attributes like `source`, `params`, `fields`
- do not include `query` in payload; `query` always comes from the child query argument
- use `nil` or omit the third argument when payload round-trip is not needed

The bare constructors are also available when the callback will be attached by
the embedding application before search execution:

```go
filterQuery := query.NewCustomFilterQuery(childQuery)
scoreQuery := query.NewCustomScoreQuery(childQuery)
```

## Runtime Behavior

- Bleve builds the child query searcher first.
- `custom_filter` wraps it with `FilteringSearcher`.
- `custom_score` wraps it with `CustomScoreSearcher`.
- the attached callback is reused for each hit during `Next()` / `Advance()`.

## Error Cases

- missing child query:
  - `custom filter query must have a query`
  - `custom score query must have a query`
- missing bound callback:
  - `custom filter query must have a filter callback`
  - `custom score query must have a score callback`
