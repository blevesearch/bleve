# Aggregations

## Overview

Bleve supports both metric and bucket aggregations with support for nested sub-aggregations. Aggregations are computed during query execution using a visitor pattern that processes only documents matching the query filter.

## Architecture

### Execution Model

Aggregations are computed inline during document collection using the visitor pattern:

1. Query execution identifies matching documents
2. For each matching document, field values are visited via `DocValueReader.VisitDocValues()`
3. Each aggregation's `UpdateVisitor()` method processes the field value
4. Results are accumulated in memory during the search
5. Final results are computed and returned with the `SearchResult`

This design ensures:
- Zero additional I/O overhead (piggybacks on existing field value visits)
- Only matching documents are aggregated
- Constant memory usage per aggregation
- Thread-safe operation across segments

### Type Hierarchy

```
AggregationBuilder (interface)
├── Metric Aggregations
│   ├── SumAggregation
│   ├── AvgAggregation
│   ├── MinAggregation
│   ├── MaxAggregation
│   ├── CountAggregation
│   ├── SumSquaresAggregation
│   ├── StatsAggregation
│   └── CardinalityAggregation (HyperLogLog++)
└── Bucket Aggregations
    ├── TermsAggregation
    ├── RangeAggregation
    ├── DateRangeAggregation
    ├── SignificantTermsAggregation
    ├── HistogramAggregation
    ├── DateHistogramAggregation
    ├── GeohashGridAggregation
    └── GeoDistanceAggregation
```

Each bucket aggregation can contain sub-aggregations, enabling hierarchical analytics.

## Aggregation Types

### Metric Aggregations

Metric aggregations compute a single numeric value from field values.

#### sum
Computes the sum of all numeric field values.

```go
agg := bleve.NewAggregationRequest("sum", "price")
```

#### avg
Computes the arithmetic mean of numeric field values.

```go
agg := bleve.NewAggregationRequest("avg", "rating")
```

#### min / max
Computes the minimum or maximum numeric field value.

```go
minAgg := bleve.NewAggregationRequest("min", "price")
maxAgg := bleve.NewAggregationRequest("max", "price")
```

#### count
Counts the number of field values.

```go
agg := bleve.NewAggregationRequest("count", "items")
```

#### sumsquares
Computes the sum of squares of field values. Useful for computing variance.

```go
agg := bleve.NewAggregationRequest("sumsquares", "values")
```

#### stats
Computes comprehensive statistics: count, sum, avg, min, max, sum_squares, variance, and standard deviation.

```go
agg := bleve.NewAggregationRequest("stats", "price")

// Result structure:
type StatsResult struct {
    Count      int64
    Sum        float64
    Avg        float64
    Min        float64
    Max        float64
    SumSquares float64
    Variance   float64
    StdDev     float64
}
```

#### cardinality
Computes approximate unique value count using HyperLogLog++. Provides memory-efficient cardinality estimation with configurable precision.

```go
agg := bleve.NewAggregationRequest("cardinality", "user_id")

// With custom precision (optional)
precision := uint8(14) // 10-18, default: 14
aggWithPrecision := &bleve.AggregationRequest{
    Type:      "cardinality",
    Field:     "user_id",
    Precision: &precision,
}

// Result structure:
type CardinalityResult struct {
    Cardinality int64  `json:"value"`  // Estimated unique count
    Sketch      []byte `json:"sketch,omitempty"` // Serialized HLL sketch
}
```

**Precision vs Accuracy Tradeoff**:
- **Precision 10**: 1KB memory, ~2.6% standard error
- **Precision 12**: 4KB memory, ~1.6% standard error
- **Precision 14**: 16KB memory, ~0.81% standard error (default)
- **Precision 16**: 64KB memory, ~0.41% standard error

**Distributed/Multi-Shard Support**:
Cardinality aggregations merge correctly across multiple index shards using HyperLogLog sketch merging, providing accurate global cardinality estimates.

### Bucket Aggregations

Bucket aggregations group documents into buckets and can contain sub-aggregations.

#### terms
Groups documents by unique field values. Returns top N terms by document count.

```go
agg := bleve.NewTermsAggregation("category", 10) // top 10 categories
```

Result structure:
```go
type Bucket struct {
    Key          interface{}            // Term value
    Count        int64                  // Document count
    Aggregations map[string]*AggregationResult // Sub-aggregations
}
```

#### range
Groups documents into numeric ranges.

```go
min := 0.0
mid := 100.0
max := 200.0

ranges := []*bleve.numericRange{
    {Name: "low", Min: nil, Max: &mid},
    {Name: "medium", Min: &mid, Max: &max},
    {Name: "high", Min: &max, Max: nil},
}

agg := bleve.NewRangeAggregation("price", ranges)
```

#### date_range
Groups documents into arbitrary date ranges. Unlike `date_histogram` which creates regular time intervals, `date_range` lets you define custom date ranges (e.g., "Q1 2023", "Summer 2024", "Pre-2020").

```go
import "time"

// Define custom date ranges
q12023 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
q22023 := time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC)
q32023 := time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC)

aggReq := &bleve.AggregationRequest{
    Type:  "date_range",
    Field: "timestamp",
    DateTimeRanges: []*bleve.dateTimeRange{
        {Name: "Q1 2023", Start: q12023, End: q22023},
        {Name: "Q2 2023", Start: q22023, End: q32023},
        {Name: "Q3 2023+", Start: q32023}, // End: zero value = unbounded
    },
}
```

**Parameters**:
- `DateTimeRanges`: Array of date ranges with Start/End as `time.Time`
- Zero value for Start = unbounded start (matches all documents before End)
- Zero value for End = unbounded end (matches all documents after Start)

**Result Structure**:
```go
// Each bucket includes start/end timestamps in metadata
type Bucket struct {
    Key:   "Q1 2023",
    Count: 1523,
    Metadata: {
        "start": "2023-01-01T00:00:00Z",  // RFC3339Nano format
        "end":   "2023-04-01T00:00:00Z",
    }
}
```

**Example Use Cases**:
- Quarterly/yearly reports with custom fiscal periods
- Seasonal analysis ("Winter 2023", "Summer 2024")
- Event-based time windows ("Before launch", "After migration")
- Arbitrary date buckets that don't fit regular intervals

**Comparison with date_histogram**:
- **date_histogram**: Regular intervals (every hour, day, month, etc.)
- **date_range**: Custom arbitrary ranges (Q1, Q2, "2020-2022", etc.)

#### significant_terms
Identifies terms that are uncommonly common in the search results compared to the entire index. Unlike `terms` aggregation which returns the most frequent terms, `significant_terms` finds terms that appear much more often in your query results than expected based on their frequency in the background data.

**Use Cases**:
- Anomaly detection: Find unusual patterns in subsets of data
- Content recommendation: Discover distinguishing characteristics
- Root cause analysis: Identify key differentiators in filtered data

**How It Works**:
1. **Two-Phase Architecture**: Uses bleve's pre-search infrastructure to collect background statistics across all index shards
2. **Foreground Collection**: During query execution, collects term frequencies from matching documents
3. **Statistical Scoring**: Compares foreground vs. background frequencies using configurable algorithms
4. **Ranking**: Returns top N terms ranked by significance score

**Statistical Algorithms**:
- **JLH** (default): Measures how "uncommonly common" a term is (high in results, low in background)
- **Mutual Information**: Information gain from knowing whether a document contains the term
- **Chi-Squared**: Statistical test for deviation from expected frequency
- **Percentage**: Simple ratio comparison of foreground to background rates

**Example**:
```go
size := 10
minDocCount := int64(5)
algorithm := "jlh" // or "mutual_information", "chi_squared", "percentage"

aggReq := &bleve.AggregationRequest{
    Type:                 "significant_terms",
    Field:                "tags",
    Size:                 &size,
    MinDocCount:          &minDocCount,
    SignificanceAlgorithm: algorithm,
}
```

**Parameters**:
- `Field`: Text field to analyze for significant terms
- `Size`: Maximum number of significant terms to return (default: 10)
- `MinDocCount`: Minimum foreground documents required (default: 1)
- `SignificanceAlgorithm`: Scoring algorithm (default: "jlh")

**Result Structure**:
```go
type Bucket struct {
    Key      string                    // The significant term
    Count    int64                     // Foreground document count
    Metadata map[string]interface{} {  // Additional statistics
        "score":    float64,           // Significance score
        "bg_count": int64,              // Background document count
    }
}

// Result metadata includes:
// - "algorithm": Algorithm used for scoring
// - "fg_doc_count": Total foreground documents
// - "bg_doc_count": Total background documents
// - "unique_terms": Number of unique terms seen
// - "significant_terms": Number of terms returned
```

**Example Scenario**:
```go
// Searching for documents about "databases"
// Background corpus: 1000 documents
//   - "programming": 300 docs (30% - very common, generic)
//   - "database": 100 docs (10% - common)
//   - "nosql": 50 docs (5% - moderately common)
//   - "scalability": 30 docs (3% - less common)
//
// Query results: 100 documents about databases
//   - "database": 95 docs (95% of results)
//   - "nosql": 45 docs (45% of results)
//   - "scalability": 25 docs (25% of results)
//   - "programming": 20 docs (20% of results)
//
// Significant terms (ranked by JLH score):
// 1. "nosql" - 45% in results vs 5% in background (9x enrichment)
// 2. "scalability" - 25% vs 3% (8.3x enrichment)
// 3. "database" - 95% vs 10% (9.5x enrichment, but already known from query)
// 4. "programming" - 20% vs 30% (0.67x - not significant, less common than expected)
```

**Performance Notes**:
- Background statistics are collected during pre-search phase across all index shards
- For single-index searches without pre-search, falls back to collecting stats from IndexReader
- Stats collection uses efficient dictionary iteration (no document reads)
- Memory usage: O(unique_terms_in_field)

#### histogram
Groups numeric values into fixed-interval buckets. Automatically creates buckets at regular intervals.

```go
interval := 50.0 // Create buckets every $50
minDocCount := int64(1) // Only show buckets with at least 1 document

aggReq := &bleve.AggregationRequest{
    Type:        "histogram",
    Field:       "price",
    Interval:    &interval,
    MinDocCount: &minDocCount,
}
```

**Parameters**:
- `Interval`: Bucket width (e.g., 50 creates buckets at 0-50, 50-100, 100-150...)
- `MinDocCount` (optional): Minimum documents required to include a bucket (default: 0)

**Example Result**:
```go
// Buckets: [0-50: 12 docs], [50-100: 45 docs], [100-150: 23 docs]
// Each bucket Key is the lower bound: 0.0, 50.0, 100.0
```

#### date_histogram
Groups datetime values into time interval buckets. Supports both calendar-aware intervals (day, month, year) and fixed durations.

**Calendar Intervals** (month-aware, DST-aware):
```go
aggReq := &bleve.AggregationRequest{
    Type:             "date_histogram",
    Field:            "timestamp",
    CalendarInterval: "1d", // 1m, 1h, 1d, 1w, 1M, 1q, 1y
}
```

**Fixed Intervals** (exact durations):
```go
aggReq := &bleve.AggregationRequest{
    Type:          "date_histogram",
    Field:         "timestamp",
    FixedInterval: "30m", // Any Go duration string
}
```

**Parameters**:
- `CalendarInterval`: Calendar-aware interval ("1m", "1h", "1d", "1w", "1M", "1q", "1y")
- `FixedInterval`: Fixed duration (e.g., "30m", "1h", "24h")
- `MinDocCount` (optional): Minimum documents required to include a bucket (default: 0)

**Example Result**:
```go
// Buckets have ISO 8601 timestamp keys
// Key: "2024-01-01T00:00:00Z", Count: 145
// Key: "2024-01-02T00:00:00Z", Count: 203
// Each bucket includes metadata with numeric timestamp
```

#### geohash_grid
Groups geo points by geohash grid cells. Useful for map visualizations and geographic analysis.

```go
precision := 5 // 5km x 5km cells
size := 10 // Return top 10 cells

aggReq := &bleve.AggregationRequest{
    Type:             "geohash_grid",
    Field:            "location",
    GeoHashPrecision: &precision,
    Size:             &size,
}
```

**Parameters**:
- `GeoHashPrecision`: Grid precision (1-12, default: 5)
  - **1**: ~5,000km x 5,000km
  - **3**: ~156km x 156km
  - **5**: ~4.9km x 4.9km (default)
  - **7**: ~153m x 153m
  - **9**: ~4.8m x 4.8m
  - **12**: ~3.7cm x 1.8cm
- `Size`: Maximum number of grid cells to return (default: 10)

**Example Result**:
```go
// Each bucket Key is a geohash string
// Metadata includes center point lat/lon
type Bucket struct {
    Key:   "9q8yy", // geohash
    Count: 1523,    // documents in this cell
    Metadata: {
        "lat": 37.7749,
        "lon": -122.4194,
    }
}
```

#### geo_distance
Groups geo points by distance ranges from a center point. Useful for "within X km" queries.

```go
from0 := 0.0
to10 := 10.0
from10 := 10.0

centerLon := -122.4194
centerLat := 37.7749

aggReq := &bleve.AggregationRequest{
    Type:         "geo_distance",
    Field:        "location",
    CenterLon:    &centerLon,
    CenterLat:    &centerLat,
    DistanceUnit: "km", // m, km, mi, ft, yd, etc.
    DistanceRanges: []*bleve.distanceRange{
        {Name: "0-10km", From: &from0, To: &to10},
        {Name: "10km+", From: &from10, To: nil}, // nil = unbounded
    },
}
```

**Parameters**:
- `CenterLon`, `CenterLat`: Center point coordinates (required)
- `DistanceUnit`: Unit for distance ranges ("m", "km", "mi", "ft", "yd", etc.)
- `DistanceRanges`: Array of distance ranges with From/To values in specified unit

**Example Result**:
```go
// Buckets sorted by distance (ascending)
// Metadata includes range boundaries and center coordinates
type AggregationResult struct {
    Buckets: [
        {Key: "0-10km", Count: 245},
        {Key: "10km+", Count: 89},
    ],
    Metadata: {
        "center_lat": 37.7749,
        "center_lon": -122.4194,
    }
}
```

## Sub-Aggregations

Bucket aggregations support nesting sub-aggregations, enabling multi-level analytics.

### Single-Level Nesting

```go
byBrand := bleve.NewTermsAggregation("brand", 10)
byBrand.AddSubAggregation("avg_price", bleve.NewAggregationRequest("avg", "price"))
byBrand.AddSubAggregation("total_revenue", bleve.NewAggregationRequest("sum", "price"))

searchRequest.Aggregations = bleve.AggregationsRequest{
    "by_brand": byBrand,
}
```

### Multi-Level Nesting

```go
byRegion := bleve.NewTermsAggregation("region", 10)

byCategory := bleve.NewTermsAggregation("category", 20)
byCategory.AddSubAggregation("total_revenue", bleve.NewAggregationRequest("sum", "price"))

byRegion.AddSubAggregation("by_category", byCategory)
```

## API Reference

### Request Structure

```go
type AggregationRequest struct {
    Type           string                // Aggregation type
    Field          string                // Field name
    Size           *int                  // For terms aggregations
    NumericRanges  []*numericRange       // For range aggregations
    Aggregations   AggregationsRequest   // Sub-aggregations
}

type AggregationsRequest map[string]*AggregationRequest
```

### Response Structure

```go
type AggregationResult struct {
    Field   string        // Field name
    Type    string        // Aggregation type
    Value   interface{}   // Metric value (for metric aggregations)
    Buckets []*Bucket     // Bucket results (for bucket aggregations)
}

type AggregationResults map[string]*AggregationResult
```

### Result Type Assertions

```go
// Metric aggregations
sum := results.Aggregations["total"].Value.(float64)
count := results.Aggregations["count"].Value.(int64)
stats := results.Aggregations["stats"].Value.(*aggregation.StatsResult)

// Bucket aggregations
for _, bucket := range results.Aggregations["by_brand"].Buckets {
    key := bucket.Key.(string)
    count := bucket.Count
    subAgg := bucket.Aggregations["avg_price"].Value.(float64)
}
```

## Query Filtering

All aggregations respect the query filter and only process matching documents.

```go
// Only aggregate documents with rating > 4.0
query := bleve.NewNumericRangeQuery(Float64Ptr(4.0), nil)
query.SetField("rating")

searchRequest := bleve.NewSearchRequest(query)
searchRequest.Aggregations = bleve.AggregationsRequest{
    "avg_price": bleve.NewAggregationRequest("avg", "price"),
}
```

## Merging Results

The `AggregationResults.Merge()` method combines results from multiple sources (e.g., distributed shards).

```go
shard1Results := search1.Aggregations
shard2Results := search2.Aggregations

// Merge shard2 into shard1
shard1Results.Merge(shard2Results)
```

Merge behavior by type:
- **sum, sumsquares, count**: Values are added
- **min**: Minimum of minimums
- **max**: Maximum of maximums
- **avg**: Approximate average (limitation: requires counts for exact merging)
- **stats**: Component values merged, derived values recalculated
- **Bucket aggregations**: Bucket counts summed, sub-aggregations merged recursively

## Comparison with Facets

Both APIs are supported and can be used simultaneously.

### Facets API (Original)
- Focused on bucketing and counting
- No sub-aggregations
- Established API with stable interface

### Aggregations API (New)
- Supports metric and bucket aggregations
- Supports nested sub-aggregations
- More flexible for complex analytics

Selection criteria:
- Use **Facets** for simple bucketing and counting
- Use **Aggregations** for metrics or nested analytics
- Both can coexist in the same query

## JSON API

Aggregations work with JSON requests:

```json
{
  "query": {"match_all": {}},
  "size": 0,
  "aggregations": {
    "by_brand": {
      "type": "terms",
      "field": "brand",
      "size": 10,
      "aggregations": {
        "avg_price": {
          "type": "avg",
          "field": "price"
        }
      }
    }
  }
}
```

Response:
```json
{
  "aggregations": {
    "by_brand": {
      "field": "brand",
      "type": "terms",
      "buckets": [
        {
          "key": "Apple",
          "doc_count": 15,
          "aggregations": {
            "avg_price": {"field": "price", "type": "avg", "value": 1099.99}
          }
        }
      ]
    }
  }
}
```

## Performance Characteristics

### Memory Usage
- **Metric aggregations**: O(1) per aggregation
- **Terms aggregations**: O(unique_terms * sub_aggregations)
- **Range aggregations**: O(num_ranges * sub_aggregations)

### Computational Complexity
- All aggregations: O(matching_documents) during execution
- Bucket aggregations: O(buckets * log(buckets)) for sorting

### Optimization Strategies

1. **Limit bucket sizes**: Use the `size` parameter to control memory usage
2. **Filter early**: Use selective queries to reduce matching documents
3. **Avoid deep nesting**: Each nesting level multiplies memory requirements
4. **Set size=0**: When only aggregations are needed, skip hit retrieval

## Implementation Details

### Numeric Value Encoding

Numeric field values are stored as prefix-coded integers for efficient range queries. Aggregations decode these values:

```go
prefixCoded := numeric.PrefixCoded(term)
shift, _ := prefixCoded.Shift()
if shift == 0 {  // Only process full-precision values
    i64, _ := prefixCoded.Int64()
    f64 := numeric.Int64ToFloat64(i64)
    // Process f64...
}
```

### Segment-Level Caching

Infrastructure exists for caching pre-computed statistics at the segment level:

```go
stats := scorch.GetOrComputeSegmentStats(segment, "price")
// Uses SegmentSnapshot.cachedMeta for storage
```

This enables future optimizations for match-all queries or repeated aggregations.

### Concurrent Execution

Aggregations process documents from multiple segments concurrently. The `TopNCollector` ensures thread-safe accumulation via:
- Separate aggregation builders per segment (if needed)
- Merge operations combine results from segments
- No shared mutable state during collection

## Limitations

1. **Pipeline aggregations**: Not yet implemented (e.g., moving average, derivative, bucket_sort)
2. **Composite aggregations**: Not yet implemented (pagination for multi-level aggregations)
3. **Nested aggregations**: Not yet implemented (requires document model changes)
4. **IP range aggregations**: Not yet implemented (ranges for IP addresses)

## Future Enhancements

- Pipeline aggregations for time-series analysis (moving averages, derivatives, cumulative sums)
- Composite aggregations for paginating through multi-level aggregations
- Automatic segment-level pre-computation for repeated queries
- Parent/child and nested document aggregations
- IP range aggregations
- Matrix stats aggregations (correlation, covariance)
