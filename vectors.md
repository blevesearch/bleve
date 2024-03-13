# bleve@v2.4.0+

* *v2.4.0* (and after) will come with support for **vectors' indexing and search**.
* We've achieved this by embedding [FAISS](https://github.com/facebookresearch/faiss) indexes within our bleve indexes.
* A new zap file format: [v16](https://github.com/blevesearch/zapx/blob/master/zap.md) - which will be the default going forward. Here we co-locate text and vector indexes as neighbors within segments, continuing to conform to the segmented architecture of *scorch*.

## Pre-requisite(s)

* Induction of [FAISS](https://github.com/blevesearch/faiss) into our eco system.
* FAISS is a C++ library that needs to be compiled and it's shared libraries need to be situated at an accessible path for your application.
* A `vectors` GO TAG needs to be set for bleve to access all the supporting code. This TAG must be set only after the FAISS shared library is made available. Failure to do either will inhibit you from using this feature.
* Please follow these [instructions](#setup-instructions) below for any assistance in the area.

## Indexing

```go
doc := struct{
    Id   string
    Text string
    Vec  []float32
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

## Caveats

* The `vector` field type is an array that is to hold float32 values only.
* Currently supported similarity metrics are: [`"l2_norm"`, `"dot_product"`].
* Supported dimensionality is between 1 and 2048 at the moment.
* Vectors from documents that do not conform to the index mapping dimensionality are simply discarded at index time.
* The dimensionality of the query vector must match the dimensionality of the indexed vectors to obtain any results.
* Pure kNN searches can be performed, but the `query` attribute within the search request must be set - to `{"match_none": {}}` in this case.
* Hybrid searches are supported, where results from `query` are unioned (for now) with results from `knn`. The tf-idf scores from exact searches are simply summed with the similarity distances to determine the aggregate scores.
```
aggregate_score = (query_boost * query_hit_score) + (knn_boost * knn_hit_distance)
```
* Multi kNN searches are supported - the `knn` object within the search request accepts an array of requests. These sub objects are unioned by default but this behavior can be overriden by setting `knn_operator` to `"and"`.
* Previously supported pagination settings will work as they were, with size/limit being applied over the top-K hits combined with any exact search hits.

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
