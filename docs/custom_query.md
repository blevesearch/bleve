# Custom Queries: `custom_filter` and `custom_score`

Bleve exposes two query nodes for embedding-defined per-hit logic:

- `custom_filter`: keep or drop each candidate hit.
- `custom_score`: mutate each candidate hit score.

Bleve itself only executes callbacks that were already provided/attached by the
embedding application. It does not interpret any
embedder-specific payload such as callback source, "params", or requested fields.

## Query Objects

`CustomFilterQuery` contains:

- a child query
- a `searcher.CustomFilterFunc` callback attached by the embedder

`CustomScoreQuery` contains:

- a child query
- a `searcher.CustomScoreFunc` callback attached by the embedder

## Constructors

Embedding applications can construct the nodes directly with callbacks:

```go
filterQuery := query.NewCustomFilterQueryWithFilter(childQuery,
    func(d *search.DocumentMatch) bool {
        return true
    },
    nil, // fields; nil when callback does not need field values
    nil, // payload; nil when no source/params round-trip is needed
)

scoreQuery := query.NewCustomScoreQueryWithScorer(childQuery,
    func(d *search.DocumentMatch) float64 {
        return d.Score
    },
    nil, // fields
    nil, // payload
)
```

Constructor arguments after the callback are:

- `fields`: field names to preload into `d.Fields` via doc values before the callback runs
- `payload`: non-query custom attributes that must survive JSON round-trips (for example distributed fan-out)

Pass `nil` for either when not needed:

```go
payload := map[string]interface{}{
    "params": map[string]interface{}{"weight": 0.05},
    "source": "function score(doc, params) { ... }",
}

scoreQuery := query.NewCustomScoreQueryWithScorer(childQuery,
    func(d *search.DocumentMatch) float64 {
        f := d.Fields
        return d.Score + (f["abv"].(float64) * 0.05)
    },
    []string{"abv", "ibu"},
    payload,
)
```

Payload rules:

- pass only non-query custom-node attributes like `source` and `params`
- do not include `query` in payload; `query` always comes from the child query argument
- do not include `fields` in payload; `fields` come from the dedicated constructor argument
- pass `nil` as the fourth argument when payload round-trip is not needed

## Field Access and Doc Values Requirement

When the `fields` constructor argument is non-empty, bleve loads the listed
field values into `DocumentMatch.Fields` before invoking the callback. Field
values are read from **doc values** (column-oriented, memory-mapped), not
stored fields.

**Doc values must be enabled for every field listed in `"fields"`.** If a field
does not have doc values enabled in the index mapping, its value will be silently
absent from `DocumentMatch.Fields` at callback time — no error is raised, the
field simply won't appear. This can cause filters to drop all hits or scores to
compute incorrectly if the callback assumes the field is present.

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

**Datetime fields require an explicit datetime mapping.** Dynamic mapping indexes
date-like strings as text, which means the standard analyzer tokenizes the value
(e.g. `"2010-07-22 20:00:20"` becomes `["2010", "07", "22", ...]`). The doc
value then contains tokens instead of the original date, making comparisons
fail. To use datetime fields in custom_filter or custom_score callbacks, add an
explicit field mapping with `"type": "datetime"` in the index definition.

### Field type decoding

At the bleve layer, doc values are decoded based on the field's mapping type:

| Mapping type | Go type in `d.Fields` | Notes |
| --- | --- | --- |
| `text` / `keyword` | `string` | Raw text bytes |
| `number` | `float64` | IEEE 754 decoded |
| `boolean` | `bool` | `true` / `false` |
| `datetime` | `float64` | Epoch nanoseconds stored as a floating-point value |

## Runtime Behavior

- Bleve builds the child query searcher first.
- If `fields` is non-empty, a `DocValueReader` is created for those fields.
- `custom_filter` wraps the child with `CustomFilterSearcher`.
- `custom_score` wraps the child with `CustomScoreSearcher`.
- On each hit, doc values are loaded into `DocumentMatch.Fields`, then the callback is invoked.
- The callback receives only `*search.DocumentMatch`; it does not receive `SearchContext`.

## Error Cases

- missing child query:
  - `"custom filter query must have a query"`
  - `"custom score query must have a query"`
- missing bound callback:
  - `"custom filter query must have a filter callback"`
  - `"custom score query must have a score callback"`
