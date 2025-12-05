# Score Fusion for Hybrid Search

Bleve supports **hybrid search** that combines full-text search (FTS) with vector (kNN) search to leverage the strengths of both approaches:

* **Full-text search** excels at exact keyword matching, filtering, and structured queries
* **Vector search** captures semantic similarity and handles synonyms and paraphrasing naturally

With *v2.5.4* onwards - when using hybrid search, you can choose different **score fusion strategies** to combine results from both search methods. This document describes the available fusion strategies and how to use them.

## Fusion Strategies

### Additive Score Fusion (Default)

By default, Bleve combines FTS and kNN scores using a simple weighted addition. See the [Vector Search documentation](vectors.md#querying) for details on the default hybrid search behavior and examples.

While this approach works well with proper boost tuning, it can be sensitive to different score scales and distributions. The fusion strategies below (RRF and RSF) provide more robust alternatives that handle score normalization automatically.

### Reciprocal Rank Fusion (RRF)

Reciprocal Rank Fusion is a **rank-based** algorithm that combines results based on their position in each result list, rather than their raw scores. This makes it robust to different score scales and distributions.

**Algorithm:**

For each document appearing in FTS or kNN results, the RRF score is calculated as:

```math
RRF\_score = w_{\text{fts}} \cdot \frac{1}{k + \text{rank}_{\text{fts}}} + \sum_{i=1}^{n} w_{\text{knn}_i} \cdot \frac{1}{k + \text{rank}_{\text{knn}_i}}
```

Where:

* $\text{rank}_{\text{fts}}$: 1-indexed rank of the document in the FTS result list (or 0 if not present)
* $\text{rank}_{\text{knn}_i}$: 1-indexed rank of the document in the i-th kNN result list (or 0 if not present)
* $k$: rank constant (default: 60) that dampens the impact of rank differences
* $w_{\text{fts}}$: weight from the FTS query boost value
* $w_{\text{knn}_i}$: weight from the i-th kNN query boost value
* $\sum_{i=1}^{n}$: summation over all kNN queries (you can add multiple kNN queries)

**Advantages:**

* Distribution-agnostic - no need for score normalization
* Works out of the box with minimal tuning
* Prioritizes documents appearing in both result lists
* Robust to outliers since only ranks matter

**Disadvantages:**

* Ignores score magnitude (loses some information)
* May be sensitive to imbalanced result list sizes

**Usage:**

```go
// Create a hybrid search with RRF fusion
searchRequest := bleve.NewSearchRequest(bleve.NewMatchQuery("dark chocolate"))
searchRequest.Score = bleve.ScoreRRF  // Alternatively, set to "rrf"

// Add first kNN component
searchRequest.AddKNN(
    "embedding",                             // Vector field
    []float32{0.1, 0.2, 0.3, 0.4},          // Query vector
    30,                                      // k neighbors
    1.0,                                     // kNN weight (boost)
)

// Add second kNN component (optional - you can add multiple)
searchRequest.AddKNN(
    "image_embedding",                       // Different vector field
    []float32{0.5, 0.3, 0.1, 0.8},          // Query vector
    20,                                      // k neighbors
    0.5,                                     // kNN weight (boost)
)

// Optional: Configure RRF parameters
params := bleve.RequestParams{
    ScoreRankConstant: 60,                   // Rank constant (default: 60)
    ScoreWindowSize: 150                     // Window size (default: size)
}
searchRequest.AddParams(params)

searchResult, err := index.Search(searchRequest)
```

### Relative Score Fusion (RSF)

Relative Score Fusion is a **score-based** strategy that normalizes scores from both modalities into a common [0, 1] range using min-max normalization before combining them.

**Algorithm:**

1. **Min-max normalize** each result set independently:

    ```math
    \text{normalized\_score} = \frac{\text{score} - \text{min\_score}}{\text{max\_score} - \text{min\_score}}
    ```

2. **Combine** normalized scores using weighted addition:

    ```math
    RSF\_score = w_{\text{fts}} \cdot \text{normalized\_score\_fts} + \sum_{i=1}^{n} w_{\text{knn}_i} \cdot \text{normalized\_score\_knn}_i
    ```

Where:

* $w_{\text{fts}}$: weight from the FTS query boost value
* $w_{\text{knn}_i}$: weight from the i-th kNN query boost value
* $\sum_{i=1}^{n}$: summation over all kNN queries (you can add multiple kNN queries)

**Advantages:**

* Score-aware - retains relevance magnitude information
* Resolves incompatible score ranges
* Easy to understand

**Disadvantages:**

* Sensitive to outliers - a single extreme score can skew normalization
* Doesn't account for the shape or distribution of scores

**Usage:**

```go
// Create a hybrid search with RSF fusion
searchRequest := bleve.NewSearchRequest(bleve.NewMatchQuery("machine learning"))
searchRequest.Score = bleve.ScoreRSF  // Or set to "rsf"

// Add first kNN component
searchRequest.AddKNN(
    "content_vector",                        // Vector field
    []float32{0.5, 0.3, 0.1, 0.8},          // Query vector
    20,                                      // k neighbors
    1.0,                                     // kNN weight (boost)
)

// Add second kNN component (optional - you can add multiple)
searchRequest.AddKNN(
    "title_vector",                          // Different vector field
    []float32{0.2, 0.7, 0.4, 0.1},          // Query vector
    15,                                      // k neighbors
    0.8,                                     // kNN weight (boost)
)

// Optional: Configure RRF parameters
params := bleve.RequestParams{
    ScoreWindowSize: 150                     // Window size (default: size)
}
searchRequest.AddParams(params)

searchResult, err := index.Search(searchRequest)
```

## Parameters

### Score

The `Score` field in your search request specifies which fusion strategy to use:

* **`ScoreRRF ("rrf")`**: Reciprocal Rank Fusion
* **`ScoreRSF ("rsf")`**: Relative Score Fusion  
* **Omitted or empty**: Default additive fusion with scores returned

### Params

The `Params` object contains additional parameters for score fusion:

#### Score Window Size

`ScoreWindowSize` is the maximum number of results to consider from each result list for fusion.

* **Default**: Same as `Size` parameter
* **Minimum**: Must be ≥ `Size` and ≥ 1
* **Purpose**: Controls the tradeoff between relevance and performance

A larger window size increases the chance of finding relevant results but requires more computation. For pagination to work consistently, ensure:

```text
From + Size <= ScoreWindowSize
```

**Example:**

```json
{
  "score": "rrf",
  "params": {
    "score_window_size": 150
  },
  "size": 10,
  "from": 0
}
```

With window size set to 150, you can paginate through up to 150 results. If you try to access results beyond this (e.g., `from=160`), you'll get an empty result set.

#### Score Rank Constant

> *Only applicable for RRF*

`ScoreRankConstant` controls how much the rank position affects the reciprocal rank score.

* **Default**: 60
* **Range**: Any positive integer
* **Effect**: Higher values dampen the impact of rank differences

**Example:**

```json
{
  "score": "rrf",
  "params": {
    "score_rank_constant": 60
  }
}
```

## Weighting Queries

The boost value in your query components controls their relative importance in hybrid search:

```go
// FTS query with boost 2.0
query := bleve.NewMatchQuery("search term")
query.SetBoost(2.0)

searchRequest := bleve.NewSearchRequest(query)

// kNN query with boost 1.0
searchRequest.AddKNN("vec", queryVector, 10, 1.0)
```

For RRF and RSF, weights determine the **relative importance** of each component's contribution, rather than scaling raw scores.

**Example:** If `fts_boost = 2.0` and `knn_boost = 1.0`, the FTS contribution is twice as important as the kNN contribution in the final ranking in RRF or RSF.

## Restrictions

When using score fusion (`Score` set to `"rrf"` or `"rsf"`), certain features are not supported:

* **SearchAfter/SearchBefore**: Not compatible with score fusion. For pagination, use `From` and `Size` only.
* **Sort**: Only descending score sort (`-_score`) or default sorting is allowed
* **Faceting**: Only documents included in the FTS result list are considered. Documents that appear exclusively in the KNN result list are ignored during faceting.

## Choosing a Fusion Strategy

| Use Case | Recommended Strategy |
|----------|---------------------|
| Different score scales (e.g., TF-IDF + L2 distance) | **RRF/RSF** |
| Minimal tuning, out-of-the-box performance | **RRF** |
| Want to preserve score magnitude importance | **RSF** |
| Have well-tuned boost values already | **Additive (default)** |
| Score distributions have extreme outliers | **RRF** |
