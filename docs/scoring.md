# Scoring models for document hits

* Default scoring scheme for document hits involving text hits: `tf-idf`.
* Vector hits scoring depends on chosen knn distance metric, highlighted [here](https://github.com/blevesearch/bleve/blob/master/docs/vectors.md#supported).
* Hybrid search scoring will combine `tf-idf` scores with `knn` distances.
* *v2.5.0* (and after) will come with **bm25 scoring** model.

## BM25

`<wip>`
