# Nearest neighbor (vector) search

* *v2.4.0* (and after) will come with support for **vectors' indexing and search**.
* We've achieved this by embedding [FAISS](https://github.com/facebookresearch/faiss) indexes within our bleve (scorch) indexes.
* Introduction of a new zap file format: [v16](https://github.com/blevesearch/zapx/blob/master/zap.md) - which will be the default going forward. Here we co-locate text and vector indexes as neighbors within segments, continuing to conform to the segmented architecture of *scorch*.

## Pre-requisite(s)

* Induction of [FAISS](https://github.com/blevesearch/faiss) into our eco system, which is a fork of the original [facebookresearch/faiss](https://github.com/facebookresearch/faiss)
* FAISS is a C++ library that needs to be compiled and it's shared libraries need to be situated at an accessible path for your application.
* A `vectors` GO TAG needs to be set for bleve to access all the supporting code. This TAG must be set only after the FAISS shared library is made available. Failure to do either will inhibit you from using this feature.
* Please follow these [instructions](#setup-instructions) below for any assistance in the area.
* Releases of `blevesearch/bleve` work with select checkpoints of `blevesearch/faiss` owing to API changes and improvements (tracking over the `bleve` branch):

    | bleve version(s) | blevesearch/faiss version |
    | --- | --- |
    | `v2.4.0` | [blevesearch/faiss@7b119f4](https://github.com/blevesearch/faiss/tree/7b119f4b9c408989b696b36f8cc53908e53de6db) (modified v1.7.4) |
    | `v2.4.1`, `v2.4.2` | [blevesearch/faiss@d9db66a](https://github.com/blevesearch/faiss/tree/d9db66a38518d99eb334218697e1df0732f3fdf8) (modified v1.7.4) |
    | `v2.4.3`, `v2.4.4` | [blevesearch/faiss@b747c55](https://github.com/blevesearch/faiss/tree/b747c55a93a9627039c34d44b081f375dca94e57) (modified v1.8.0) |
    | `v2.5.0`, `v2.5.1` | [blevesearch/faiss@352484e](https://github.com/blevesearch/faiss/tree/352484e0fc9d1f8f46737841efe5f26e0f383f71) (modified v1.10.0) |
    | `v2.5.2`, `v2.5.3`, `v2.5.4` | [blevesearch/faiss@b3d4e00](https://github.com/blevesearch/faiss/tree/b3d4e00a69425b95e0b283da7801efc9f66b580d) (modified v1.11.0) |
    | `v2.5.5`, `v2.5.6`, `v2.5.7` | [blevesearch/faiss@8a59a0c](https://github.com/blevesearch/faiss/tree/8a59a0c552fa2d14fa871f6b6bc793de1d277f5e) (modified v1.12.0) |
    | `v2.6.0` | [blevesearch/faiss@608356b](https://github.com/blevesearch/faiss/tree/608356b7c9630e891ff87cc49cc7bb460c3870d3) (modified v1.13.1) |

## Supported

* The `vector` field type is an array that is to hold float32 values only.
* The `vector_base64` field type to support base64 encoded strings using little endian byte ordering (v2.4.1+)
* Supported similarity metrics are: [`"cosine"` (v2.4.3+), `"dot_product"`, `"l2_norm"`].
  * `cosine` paths will additionally normalize vectors before indexing and search.
* Supported dimensionality is between 1 and 2048 (v2.4.0), and up to **4096** (v2.4.1+).
* Supported vector index optimizations: `latency`, `memory_efficient` (v2.4.1+), `recall`.
* Vectors from documents that do not conform to the index mapping dimensionality are simply discarded at index time.
* The dimensionality of the query vector must match the dimensionality of the indexed vectors to obtain any results.
* Pure kNN searches can be performed, but the `query` attribute within the search request must be set - to `{"match_none": {}}` in this case. The `query` attribute is made optional when `knn` is available with v2.4.1+.
* Hybrid searches are supported, where results from `query` are unioned (for now) with results from `knn`. The tf-idf scores from exact searches are simply summed with the similarity distances to determine the aggregate scores.

```text
aggregate_score = (query_boost * query_hit_score) + (knn_boost * knn_hit_distance)
```

* Advanced score fusion strategies (v2.5.4+) are available if requested for - see [score fusion](score_fusion.md#score-fusion-for-hybrid-search).
* Multi kNN searches are supported - the `knn` object within the search request accepts an array of requests. These sub objects are unioned by default but this behavior can be overridden by setting `knn_operator` to `"and"`.
* Previously supported pagination settings will work as they were, with size/limit being applied over the top-K hits combined with any exact search hits.
* Pre-filtered vector and hybrid search (v2.4.3+): Apply any Bleve filter query first to narrow down candidates before running kNN search, making vector and hybrid searches faster and more relevant.
* Fields containing multiple vectors (v2.5.7+):
  * A single document may contain multiple vectors within the same field, in the form of either:
    * an array of vectors (multi-vector field)
    * an array of objects each containing a vector (nested-vector field)
  * **All vectors in the field must share the same dimensionality**.
  * For single-kNN queries, each document is scored using its single best-matching vector.
  * For multi-kNN queries, the system selects the best-matching vector for each query vector within the document.

## Indexing

```go
// Example document with single-vector, multi-vector, and nested-vector fields
doc := struct {
    Id         string      `json:"id"`
    Text       string      `json:"text"`
    Vec        []float32   `json:"vec"`        // Single-vector field
    Embeddings [][]float32 `json:"embeddings"` // Multi-vector field: array of vectors (v2.5.7+)
    Sections   []struct {  // Nested-vector field: array of objects with vectors (v2.5.7+)
        Text string `json:"text"`
        Vec  []float32 `json:"vec"`
    } `json:"sections"`
}{
    Id:   "example",
    Text: "hello from united states",
    Vec:  []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, // Single-vector field of dimensionality 10
    Embeddings: [][]float32{ // Multi-vector field containing 2 vectors of dimensionality 10
        {10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, // First vector
        {20, 21, 22, 23, 24, 25, 26, 27, 28, 29}, // Second vector
    },
    Sections: []struct { // Nested-vector field containing 2 objects each with a vector of dimensionality 10
        Text string `json:"text"`
        Vec  []float32 `json:"vec"`
    }{
        {Text: "first section", Vec: []float32{30, 31, 32, 33, 34, 35, 36, 37, 38, 39}},
        {Text: "second section", Vec: []float32{40, 41, 42, 43, 44, 45, 46, 47, 48, 49}},
    },
}

// Field mappings
textFieldMapping := bleve.NewTextFieldMapping()
vectorFieldMapping := bleve.NewVectorFieldMapping()
vectorFieldMapping.Dims = 10              // Set vector dimensionality
vectorFieldMapping.Similarity = "l2_norm" // Euclidean distance

// Sub-document mappings
sectionsMapping := bleve.NewDocumentMapping()
sectionsMapping.AddFieldMappingsAt("text", textFieldMapping)
sectionsMapping.AddFieldMappingsAt("vec", vectorFieldMapping)

// Index mapping
bleveMapping := bleve.NewIndexMapping()
bleveMapping.DefaultMapping.AddFieldMappingsAt("text", textFieldMapping)
bleveMapping.DefaultMapping.AddFieldMappingsAt("vec", vectorFieldMapping)        // Single-vector
bleveMapping.DefaultMapping.AddFieldMappingsAt("embeddings", vectorFieldMapping) // Multi-vector
bleveMapping.DefaultMapping.AddSubDocumentMapping("sections", sectionsMapping)   // Nested-vector

// Create the index
index, err := bleve.New("example.bleve", bleveMapping)
if err != nil {
    panic(err)
}

// Index the document
err = index.Index(doc.Id, doc)
if err != nil {
    panic(err)
}
```

## Querying

```go
// ------------------------------------
// Single-vector field search (v2.4.0+)
// ------------------------------------
searchRequest := bleve.NewSearchRequest(bleve.NewMatchNoneQuery())
searchRequest.AddKNN(
    "vec",                                   // Vector field
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9}, // Query vector
    5,                                       // top-k
    1,                                       // boost
)
searchResult, err := index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Scores are 1 / squared L2 distance, e.g., score = 0.25 for squared distance of 4
fmt.Printf("Single-vector field kNN result:\n%s\n", searchResult)
```

```go
// -----------------------------------
// Multi-vector field search (v2.5.7+)
// -----------------------------------
searchRequest = bleve.NewSearchRequest(bleve.NewMatchNoneQuery())
searchRequest.AddKNN(
    "embeddings",
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9},
    5,
    1.0,
)
searchResult, err = index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Scores are based on the **best-matching vector** from the multi-vector field.
// Example: distances to doc vectors {10..19} and {20..29} → pick the closer one (smaller squared L2),
// then score = 1 / squared L2 distance.
fmt.Printf("Multi-vector field kNN result:\n%s\n", searchResult)
```

```go
// ------------------------------------
// Nested-vector field search (v2.5.7+)
// ------------------------------------
searchRequest = bleve.NewSearchRequest(bleve.NewMatchNoneQuery())
searchRequest.AddKNN(
    "sections.vec",
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9},
    5,
    1.0,
)
searchResult, err = index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Scores are based on the **best-matching vector** from the nested-vector field.
// Example: distances to doc vectors {30..39} and {40..49} → pick the closer one (smaller squared L2),
// then score = 1 / squared L2 distance.
fmt.Printf("Nested-vector field kNN result:\n%s\n", searchResult)
```

```go
// -----------------------------------------------------
// Multi kNN queries on multi-vector documents (v2.5.7+)
// -----------------------------------------------------
searchRequest = bleve.NewSearchRequest(bleve.NewMatchNoneQuery())
searchRequest.AddKNN(
    "embeddings",
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9},
    5,
    1.0,
)
searchRequest.AddKNN(
    "embeddings",
    []float32{1, 2, 2, 5, 5, 6, 8, 7, 9, 10},
    8,
    1.0,
)
searchResult, err = index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Document score explanation:
// - For each query vector, Bleve selects the **closest vector** in the multi-vector field.
// - Scores from multiple queries are then **normalized and summed** to get the final document score.
// For example, if the closest vector to the first query has squared L2 distance 4 (score 0.25)
// and the closest vector to the second query has squared L2 distance 1 (score 1.0),
// and both queries use equal boost values of 1.0, the normalization factor is 1/√2 (where 2 is the number of kNN queries).
// Then the total document score = 1/√2 * 0.25 + 1/√2 * 1.0 = 0.1768 + 0.7071 = 0.8839.
// Note: If the boost values differ, or if more queries are used, the normalization factor and score calculation will change accordingly.
fmt.Printf("Multi kNN queries result:\n%s\n", searchResult)
```

```go
// --------------------------------------
// Hybrid search: text + vector (v2.4.0+)
// --------------------------------------
searchRequest = bleve.NewSearchRequest(bleve.NewMatchQuery("united states"))
searchRequest.AddKNN(
    "vec",
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9},
    5,
    1,
)
searchResult, err = index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Score = sum of text relevance score + kNN vector score
// Example: text score 0.5 + vector score 0.25 = total score 0.75
fmt.Printf("Hybrid search result:\n%s\n", searchResult)
```

## Querying with filters (v2.4.3+)

```go
// Pre-filtered vector/hybrid search: filter query narrows candidates before KNN search
searchRequest = bleve.NewSearchRequest(bleve.NewMatchNoneQuery()) // replace with any Bleve query for Pre-filtered Hybrid Search
filterQuery := bleve.NewTermQuery("hello")                        // Filter query to narrow candidates
searchRequest.AddKNNWithFilter(
    "vec",                                   // Vector field name
    []float32{0, 1, 1, 4, 4, 5, 7, 6, 8, 9}, // Query vector (must match indexed vector dims)
    5,                                       // Number of nearest neighbors to return (k)
    1,                                       // Boost factor for KNN score
    filterQuery,                             // Filter query applied before KNN search
)
searchResult, err = index.Search(searchRequest)
if err != nil {
    panic(err)
}
// Scores are computed only among documents matching the filter query
// Example: if only one document matches the filter and has squared L2 distance 4 to the query vector,
// its score will be 0.25 (1 / 4) and returned as the top result.
fmt.Printf("Pre-filtered kNN search result:\n%s\n", searchResult)
```

## Setup Instructions

* Using `cmake` is a recommended approach by FAISS authors.
* More details here - [faiss/INSTALL](https://github.com/blevesearch/faiss/blob/main/INSTALL.md).

### Linux

Also documented here - [go-faiss/README](https://github.com/blevesearch/go-faiss/blob/master/README.md).

```shell
git clone https://github.com/blevesearch/faiss.git
cd faiss
cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=ON .
make -C build
sudo make -C build install
```

Building will produce the dynamic library `faiss_c`. You will need to install it in a place where your system will find it (e.g. /usr/lib). You can do this with:

```shell
sudo cp build/c_api/libfaiss_c.so /usr/local/lib
```

### OSX

While you shouldn't need to do any different over osX x86_64, with aarch64 - some instructions need adjusting (see [facebookresearch/faiss#2111](https://github.com/facebookresearch/faiss/issues/2111)) ..

```shell
LDFLAGS="-L/opt/homebrew/opt/llvm/lib" CPPFLAGS="-I/opt/homebrew/opt/llvm/include" CXX=/opt/homebrew/opt/llvm/bin/clang++ CC=/opt/homebrew/opt/llvm/bin/clang cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=ON -DFAISS_ENABLE_PYTHON=OFF .
make -C build
sudo make -C build install
sudo cp build/c_api/libfaiss_c.dylib /usr/local/lib
```

### Sanity check

Once the supporting library is built and made available, a sanity run is recommended to make sure all unit tests and especially those accessing the vectors' code pass. Here's how ..

```shell
go test -ldflags "-r /usr/local/lib" ./... -tags=vectors
```
