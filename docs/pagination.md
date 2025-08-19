# Pagination

## Why pagination matters

Search queries can match many documents. Pagination lets you fetch and display results in chunks, keeping responses small and fast. 

By default, Bleve returns the first 10 hits sorted by relevance (score), highest first.

## Two pagination modes

- `From`/`Size`: simple and stateless; cost grows with page depth.
- `SearchAfter`/`SearchBefore`: efficient for deep paging; requires passing sort keys from the previous page.

Both modes can be used with any valid sort.

## `Size`/`From`

Offset-based pagination uses `Size` (page length) and `From` (number of hits to skip). Bleve collects at least `Size + From` ordered results, then returns the `Size` slice starting at `From`.

JSON example:

```json
{
  "query": { "match": "California" },
  "sort": ["-_score"],
  "size": 5,
  "from": 10
}
```

The result would be 5 hits starting from the 5th hit.

When to use:

- Simple, stateless pagination for shallow pages.
- Avoid for deep pages, as memory grows with `From` for deeper pages.

## `SearchAfter` and `SearchBefore`

This returns the next (or previous) page based on a boundary defined by the sort keys of a specific hit. This keeps resource usage proportional to the page size, even for deep pages.

Rules:

- Use either `SearchAfter` (forward) or `SearchBefore` (backward), not both at once.
- The length of `SearchAfter`/`SearchBefore` must match the length of `Sort`.
- Values are strings representing the sort keys, in the same order as `Sort`.
- Keep the same `query` and `sort` across pages for consistent navigation.

Where do sort keys come from?

- Each hit includes `Sort` (and `DecodedSort` from Bleve v2.5.2). Take the last hit’s sort keys for `SearchAfter`, or the first hit’s sort keys for `SearchBefore`.
- If the field/fields to be searched over is numeric, datetime or geo, the values in the `Sort` field may have garbled values; this is because of how Bleve represents such data types internally. To use such fields as sort keys, use the `DecodedSort` field, which decodes the internal representations. This feature is available from Bleve v2.5.4.

> When using `DecodedSort`, the `Sort` array in the search request needs to explicitly declare the type of the field for proper decoding. Hence, the `Sort` array must contain either `SortField` objects (for numeric and datetime) or `SortGeoDistance` objects (for geo) rather than just the field names. More info on `SortField` and `SortGeoDistance` can be found in [sort_facet.md](sort_facet.md).

Forward pagination over `_id` and `_score`:

```json
{
  "query": { "match": "California" },
  "sort": ["_id", "_score"],
  "search_after": ["hotel_10180", "0.998"],
  "size": 3
}
```

Backward pagination over `_id` and `_score`:

```json
{
  "query": { "match": "California" },
  "sort": ["_id", "_score"],
  "search_before": ["hotel_17595", "0.623"],
  "size": 4
}
```

Pagination using numeric, datetime and geo fields. Notice how we specify the sort objects, with the "type" field explicitly declared in case of numeric and datetime:
```json
{
  "query": {
    "match_all": {}
  },
  "size": 10,
  "sort": [
    {"by": "field", "field": "price", "type": "number"},
    {"by": "field", "field": "created_at", "type": "date"},
    {"by": "geo_distance", "field": "location", "location": {"lat": 40.7128,"lon": -74.0060}}
  ],
  "search_after": ["99.99", "2023-10-15T10:30:00Z", "5.2"]
}

```
## Total Sort Order

Pagination is deterministic. Ensure your `Sort` defines a total order, so that documents with the same sort keys are not left out:

- Sort strings can be field names (prefix with `-` for descending), `"_score"`, or `"_id"`.
- Always include a stable tie-breaker as the last key, typically `"_id"`.
- Examples:
  - `["country", "-age", "_id"]`
  - `["-_score", "_id"]` (default score desc with a tie-breaker)

## Performance guidance

- Offset pagination cost grows with `From` (collects at least `Size + From` results before slicing).
- `SearchAfter`/`SearchBefore` keeps memory and network proportional to `Size`.
- For large datasets and deep navigation, prefer using `SearchAfter` and `SearchBefore`.