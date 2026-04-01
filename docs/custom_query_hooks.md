# Custom Query Hooks: `custom_filter` and `custom_score`

Bleve exposes two query nodes for embedding-defined per-hit logic:

- `custom_filter`: keep or drop each candidate hit.
- `custom_score`: mutate each candidate hit score.

Bleve itself only executes already-bound callbacks. It does not interpret any
embedder-specific payload such as callback source, params, or requested fields.

## Query Objects

`CustomFilterQuery` contains:

- a child query
- a bound `searcher.FilterFunc`

`CustomScoreQuery` contains:

- a child query
- a bound `searcher.ScoreFunc`

## Constructors

Embedding applications can construct the nodes directly with callbacks:

```go
filterQuery := bleve.NewCustomFilterQueryWithFilter(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
		return true
	},
)

scoreQuery := bleve.NewCustomScoreQueryWithScorer(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
		return d.Score
	},
)
```

The bare constructors are also available when the callback will be attached by
the embedding application before search execution:

```go
filterQuery := bleve.NewCustomFilterQuery(childQuery)
scoreQuery := bleve.NewCustomScoreQuery(childQuery)
```

## Runtime Behavior

- Bleve builds the child query searcher first.
- `custom_filter` wraps it with `FilteringSearcher`.
- `custom_score` wraps it with `ScoreMutatingSearcher`.
- the bound callback is reused for each hit during `Next()` / `Advance()`.

## Error Cases

- missing child query:
  - `custom filter query must have a query`
  - `custom score query must have a query`
- missing bound callback:
  - `custom filter query must have a filter callback`
  - `custom score query must have a score callback`
