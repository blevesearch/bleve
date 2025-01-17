# Scoring models for document hits

* Search is performed on a collection fields using compound queries such as conjunction/disjunction/boolean etc. However, the scoring itself is done independently for each field and then aggregated to get the final score for a document hit.
* Default scoring scheme for document hits involving text hits: `tf-idf`.
* Nearest-neighbor/vector hits scoring depends on chosen `knn distance` metric, highlighted [here](https://github.com/blevesearch/bleve/blob/master/docs/vectors.md#supported).
* Hybrid search scoring will combine `tf-idf` scores with `knn distance` numbers.
* *v2.5.0* (and after) will come with support for `bm25` scoring for exact searches.

## BM25

When it comes to scoring a document hit for a specific field, BM25 scoring mechanism requires the following stats:
* fieldLen - The number of analyzed terms in the current document's field.
* avgFieldLen - The average number of analyzed terms in the field across all the documents.
* docTotal - The total number of documents in the index.
* docTerm - The total number of documents containing the query term within the index.


The scoring formula followed in BM25 is

```math
\sum_{i}^n IDF(q_i) {{f(q_i,D) * (k1 + 1)}\over{f(q_i,D) + k1 * (1-b+b*{{fieldLen}\over{avgFieldLen}})}}
```

$IDF(q_i)$ here refers to Inverse Document Frequency talks about how rare (and hence rich in information) is a particular query term $`q_i`$ across all the documents in the index, which is calculated as
```math
    \ln(1 + {{docTotal - docTerm + 0.5}\over{docTerm + 0.5}})
```

Coming back to the BM25 scoring, $f(q_i,D)$ refers to the frequency of the query term in document $D$. The entire equation has certain multipliers 
* $k1$ - helps in controlling the saturation of the score with respect to query term in a document. Basicaly if the query term's frequency is too high, the score value gets saturated and doesn't increase beyond a certain point.
* $b$ - controls the extent to which the $fieldLen$ normalizes the term's frequency. 

### How to enable and use BM25

Bleve v2.5.0 updated the `indexMapping` construct with the concept of `scoringModel`. This is a global (meaning applicable to all the fields) setting which drives which scoring algorithm to apply while scoring the document hits. Supported scoring models are defined [here](https://github.com/blevesearch/bleve_index_api/blob/f54d76f0a71a838837159aa44ced0404bb6ec25f/indexing_options.go#L27)

For instance, while defining the index mapping for the data model that's been decided by the user, following snippet can be referred to enable BM25

```go
indexMapping := bleve.NewIndexMapping()
indexMapping.TypeField = "type"
indexMapping.DefaultAnalyzer = "en"
indexMapping.ScoringModel = index.BM25Scoring
```

During search time there's explicit change involved, unless the user wants to perform a global scoring.

### Global Scoring

Let's say that the user has a dataset which is quite large (let's say 3 million) and to have good throughput, they create 3 shards (with the same index mapping) for the "index". Each of these shards can be `bleve.Index` type and while performing a search over the entire dataset, a `bleve.IndexAlias` can be created over which a search can be performed. This parallelizes things pretty good, both on the indexing path and the search path.

The concept of global scoring is applicable when the index is "sharded" (similar to above situation). This is because each index has data which is disjoint, and thereby while performing the scoring on document hits on each of them, the value of stats is not complete at a global level, since we're doing a search over the entire dataset using the `bleve.IndexAlias`. For eg: `docTotal` value while scoring the document hits would be 1 million which is incorrect at a global level. 

So in order to keep the scoring roughly same across varying count of the number of shards involved, we provide a mechanism to enable "global scoring". In this type of search, an initial roundtrip is performed to gather and aggregate the stats necessary for the scoring mechanism and in the second phase, the actual search is performed. So naturally this comes at a cost of latency. As a reference here's how the user can go about with it

```go
multiPartIndex := bleve.NewIndexAlias(shard1, shard2)
// set the alias with the same index mapping which both the shards use.
err = multiPartIndex.SetIndexMapping(indexMapping)
if err != nil {
    return err
}

ctx := context.Background()
ctx = context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)

res, err := multiPartIndex.SearchInContext(ctx, searchRequest)
```

A note here is that, this would only matter if the relative order of the document hits vary quite a bit (vs single shard case). This would be possible when the shard count increases quite a bit, in low doc count situations or if there is a heavy skew in the data distribution amongst the shards for some reason. Ideally the shards are created when the data is quite large and each of them index same amount of data - in which case the scores won't fluctuate much to affect the relative hit order and the user can choose to avoid the global scoring mechanism altogether. 

## TF-IDF

TF-IDF is the default scoring mechanism involved (for backward compatibility reasons) and requires no change from the user at index or search time to avail it. 

The scoring formula involved is

```math
    \sum_{i}^n f(q_i, D) * {{1}\over{\sqrt{fieldLen}}} * IDF(q_i)
```

where $IDF(q_i)$ is

```math
    1 + {{docTotal}\over{1 + docTerm}}
```

Note: TF-IDF formula doesn't accomodate logic for score saturation due to term frequency or fieldLen. So, it's recommended to use BM25 scoring by explicity setting it in the index mapping.