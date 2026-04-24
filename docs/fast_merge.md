# Fast Merge

* v2.6.0 comes with the support for a feature called "fast merge" where we first train the index on a vector dataset and then build the vector index using this trained information of the centroid layout.
* This is an improvement on the existing behavior where we were performing the merge in a very naive fashion of reconstructing the participating vector indexes, re-training and then adding back the vectors into the index. Fast merge essentially merges the corresponding centroid cells' data vectors in a block wise fashion without doing the expensive operations
* This feature underneath the hood utilizes the existing [`merge_from` API](https://github.com/blevesearch/faiss/blob/ffd910a91f1acf49b9898a7e514e462db89ee7b3/faiss/Index.h#L396) in our fork of the faiss codebase.

## Support

* This feature is supported primarily for the IVF family of indexes. This happens when
  * the field mapping has optimization type as `ivf,rabitq` or when using `bivf-sq8`/`bivf-flat` for binary quantization.
  * when the above optimizations aren't used but the scale of data exceeds 10K vectors.

## Usage

The feature can be enabled by first passing a key value pair in the config part while creating a new index. If the flag is false, then the behavior falls back to the more expensive naive merge.

```go
kvConfig := map[string]interface{}{
  "vector_index_fast_merge": true,
}

index, err := bleve.NewUsing("example.bleve", bleve.NewIndexMapping(), bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, kvConfig)
if err != nil {
  log.Fatal(err)
}
```

User should now "train" the index on a random sample of the vector dataset they're planning to index and search.

* It's completely up to the user as to how much data they want to use for training, controlling the batch size used while training and also marking whether the training is complete.
* NOTE: User must index their data only after marking the training as complete, otherwise the batch won't be indexed.

The training is done using the new `Train()` API which takes the existing `Batch` construct we have

```go
batch := index.NewBatch()
for _, doc := range trainingDocuments {
  batch.Index(doc.ID, doc)
}

// train the index on the batch of data
// NOTE: the training can be done in an incremental manner as well, by using same Train() API but repeatedly calling it on a particular batch of data. 
if err := index.Train(batch); err != nil {
  log.Fatal(err)
}

batch.Reset()
batch.SetInternal(util.BoltTrainCompleteKey, []byte("true"))
if err := index.Train(batch); err != nil {
  log.Fatal(err)
}

// at this point the index is trained completely, and any training related constructs in the background will be cleaned up
```

## Disclaimer

* This feature is primarily meant for the use case where the user is aware about much data they want to index and also for a ready heavy workload and little to no updates on the index itself.
* The intention of the feature is to be able to quickly index a massive scale of data on an index in an expensive manner and perform search on it.
* Without this feature, i.e. when the index build happens without a prior training phase
  * The user wouldn't have to worry about use cases where the dataset is continuously updated with new "type" of vector. This is because each merge cycle would do the training afresh.
  * The user doesn't have a lag in indexing the data either, they can start ingesting the data immediately.
* Based on what's mentioned above, when it comes to update and delete type of workloads on the dataset its extremely difficult to detect when the data drift will occur. So we end up falling back to the naive way of reconstructing + re-training.
