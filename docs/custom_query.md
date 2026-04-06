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
	nil, // payload; nil when no fields/params/source are needed
)

scoreQuery := query.NewCustomScoreQueryWithScorer(childQuery,
	func(sctx *search.SearchContext, d *search.DocumentMatch) float64 {
		return d.Score
	},
	nil,
)
```

The third argument is a payload map that carries fields, params, and source through
JSON round-trips (e.g. distributed fan-out). Pass nil when not needed:

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
	payload,
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

## Field Access and Doc Values Requirement

When the payload includes a `"fields"` key, bleve loads the listed field values
into `DocumentMatch.Fields` before invoking the callback. Field values are read
from **doc values** (column-oriented, memory-mapped), not stored fields.

**Doc values must be enabled for every field listed in `"fields"`.** If a field
does not have doc values enabled in the index mapping, its value will not be
available to the callback at query time.

Doc values are enabled by default for all field types (`DocValues: true`) and
for dynamic mappings (`DocValuesDynamic: true`), so no extra configuration is
needed unless these defaults have been explicitly disabled. If they have been
disabled, re-enable them:

```go
fm := mapping.NewTextFieldMapping()
fm.DocValues = true                    // default is true; only needed if previously disabled
imap.DefaultMapping.AddFieldMappingsAt("genre", fm)
```

For dynamic mappings, ensure `docvalues_dynamic` has not been set to `false` on
the document mapping.

### Field type decoding

At the bleve layer, doc values are decoded based on the field's mapping type:

| Mapping type | Go type in `d.Fields` | Notes |
|---|---|---|
| `text` / `keyword` | `string` | Raw text bytes |
| `number` | `float64` | IEEE 754 decoded |
| `boolean` | `bool` | `true` / `false` |
| `datetime` | `string` | RFC 3339 formatted (e.g. `"2022-03-10T00:00:00Z"`) |

## Runtime Behavior

- Bleve builds the child query searcher first.
- If `"fields"` is present in the payload, a `DocValueReader` is created for those fields.
- `custom_filter` wraps the child with `CustomFilterSearcher`.
- `custom_score` wraps the child with `CustomScoreSearcher`.
- On each hit, doc values are loaded into `DocumentMatch.Fields`, then the callback is invoked.

## Error Cases

- missing child query:
  - `custom filter query must have a query`
  - `custom score query must have a query`
- missing bound callback:
  - `custom filter query must have a filter callback`
  - `custom score query must have a score callback`
