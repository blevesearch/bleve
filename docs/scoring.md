# Scoring models for document hits

* Default scoring scheme for document hits involving text hits: `tf-idf`.
* Nearest-neighbor/vector hits scoring depends on chosen `knn distance` metric, highlighted [here](https://github.com/blevesearch/bleve/blob/master/docs/vectors.md#supported).
* Hybrid search scoring will combine `tf-idf` scores with `knn distance` numbers.
* *v2.5.0* (and after) will come with support for `bm25` scoring for exact searches.

## BM25

`<wip>`
