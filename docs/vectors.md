# bleve@v2.4.0+

* *v2.4.0* (and after) will come with support for **vectors' indexing and search**.
* We've achieved this by embedding [FAISS](https://github.com/facebookresearch/faiss) indexes within our bleve (scorch) indexes.
* Introduction of a new zap file format: [v16](https://github.com/blevesearch/zapx/blob/master/zap.md) - which will be the default going forward. Here we co-locate text and vector indexes as neighbors within segments, continuing to conform to the segmented architecture of *scorch*.

## Pre-requisite(s)

* Induction of [FAISS](https://github.com/blevesearch/faiss) into our eco system, which is a fork of the original [facebookresearch/faiss](https://github.com/facebookresearch/faiss)
* FAISS is a C++ library that needs to be compiled and it's shared libraries need to be situated at an accessible path for your application.
* A `vectors` GO TAG needs to be set for bleve to access all the supporting code. This TAG must be set only after the FAISS shared library is made available. Failure to do either will inhibit you from using this feature.
* Please follow these [instructions](#setup-instructions) below for any assistance in the area.
* Releases of `blevesearch/bleve` work with select checkpoints of `blevesearch/faiss` owing to API changes and improvements (tracking over the `bleve` branch):
    * *v2.4.0* requires [blevesearch/faiss@7b119f4](https://github.com/blevesearch/faiss/tree/7b119f4b9c408989b696b36f8cc53908e53de6db) (modified v1.7.4)
    * *v2.4.1* requires [blevesearch/faiss@d9db66a](https://github.com/blevesearch/faiss/tree/d9db66a38518d99eb334218697e1df0732f3fdf8) (modified v1.7.4)
    * *v2.4.2* requires [blevesearch/faiss@d9db66a](https://github.com/blevesearch/faiss/tree/d9db66a38518d99eb334218697e1df0732f3fdf8) (modified v1.7.4)
    * *v2.4.3* requires [blevesearch/faiss@26d9b359](https://github.com/blevesearch/faiss/tree/26d9b359c7cffe51f69151dcb78ebe7a345448b0) (modified v1.8.0)

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
```
aggregate_score = (query_boost * query_hit_score) + (knn_boost * knn_hit_distance)
```
* Multi kNN searches are supported - the `knn` object within the search request accepts an array of requests. These sub objects are unioned by default but this behavior can be overriden by setting `knn_operator` to `"and"`.
* Previously supported pagination settings will work as they were, with size/limit being applied over the top-K hits combined with any exact search hits.

## Indexing

```go
doc := struct{
    Id   string    `json:"id"`
    Text string    `json:"text"`
    Vec  []float32 `json:"vec"`
}{
    Id:   "example",
    Text: "hello from united states",
    Vec:  []float32{0,1,2,3,4,5,6,7,8,9},
}

textFieldMapping := mapping.NewTextFieldMapping()
vectorFieldMapping := mapping.NewVectorFieldMapping()
vectorFieldMapping.Dims = 10
vectorFieldMapping.Similarity = "l2_norm"   // euclidean distance

bleveMapping := bleve.NewIndexMapping()
bleveMapping.DefaultMapping.Dynamic = false
bleveMapping.DefaultMapping.AddFieldMappingsAt("text", textFieldMapping)
bleveMapping.DefaultMapping.AddFieldMappingsAt("vec", vectorFieldMapping)

index, err := bleve.New("example.bleve", bleveMapping)
if err != nil {
    panic(err)
}
index.Index(doc.Id, doc)
```

## Querying

```go
searchRequest := NewSearchRequest(query.NewMatchNoneQuery())
searchRequest.AddKNN(
    "vec",                                      // vector field name
    []float32{10,11,12,13,14,15,16,17,18,19},   // query vector (same dims)
    5,                                          // k
    0,                                          // boost
)
searchResult, err := index.Search(searchRequest)
if err != nil {
    panic(err)
}
fmt.Println(searchResult.Hits)
```

## Querying with filters (v2.4.3+)

```go
searchRequest := NewSearchRequest(query.NewMatchNoneQuery())
filterQuery := NewTermQuery("hello")
searchRequest.AddKNNWithFilter(
    "vec",                                      // vector field name
    []float32{10,11,12,13,14,15,16,17,18,19},   // query vector (same dims)
    5,                                          // k
    0,                                          // boost
    filterQuery,                                // filter query
)
searchResult, err := index.Search(searchRequest)
if err != nil {
    panic(err)
}
fmt.Println(searchResult.Hits)
```

## Setup Instructions

* Using `cmake` is a recommended approach by FAISS authors.
* More details here - [faiss/INSTALL](https://github.com/blevesearch/faiss/blob/main/INSTALL.md).

### Linux

Also documented here - [go-faiss/README](https://github.com/blevesearch/go-faiss/blob/master/README.md).

```
git clone https://github.com/blevesearch/faiss.git
cd faiss
cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=ON .
make -C build
sudo make -C build install
```

Building will produce the dynamic library `faiss_c`. You will need to install it in a place where your system will find it (e.g. /usr/lib). You can do this with:
```
sudo cp build/c_api/libfaiss_c.so /usr/local/lib
```

### OSX

While you shouldn't need to do any different over osX x86_64, with aarch64 - some instructions need adjusting (see [facebookresearch/faiss#2111](https://github.com/facebookresearch/faiss/issues/2111)) ..

```
LDFLAGS="-L/opt/homebrew/opt/llvm/lib" CPPFLAGS="-I/opt/homebrew/opt/llvm/include" CXX=/opt/homebrew/opt/llvm/bin/clang++ CC=/opt/homebrew/opt/llvm/bin/clang cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=ON -DFAISS_ENABLE_PYTHON=OFF .
make -C build
sudo make -C build install
sudo cp build/c_api/libfaiss_c.dylib /usr/local/lib
```

### Sanity check

Once the supporting library is built and made available, a sanity run is recommended to make sure all unit tests and especially those accessing the vectors' code pass. Here's how I do on mac -

```
export DYLD_LIBRARY_PATH=/usr/local/lib
go test -v ./... --tags=vectors
```
