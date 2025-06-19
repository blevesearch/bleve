# Scorch Index Memory and File Management

## Memory Management

When data is indexed in Scorch — using either the `index.Index()` or `index.Batch()` API — it is added as part of an in-memory "segment". Memory management in Scorch indexing mainly relates to handling these in-memory segments during workloads that involve inserts or updates. 

In scenarios with a continuous stream of incoming data, a large number of in-memory segments can accumulate over time. This is where the persister component comes into play—its job is to flush these in-memory segments to disk.

Starting with v2.5.0, Scorch supports parallel flushing of in-memory segments to disk, where the persister checks the total in-memory data and distributes the flush across multiple workers. This feature is disabled by default and can be enabled using two configuration options:

- `NumPersisterWorkers`: This factor decides how many maximum workers can be spawned to flush out the in-memory segments. Each worker will work on a disjoint subset of segments, merge them, and flush them out to the disk. By default the persister deploys only one worker.
- `MaxSizeInMemoryMergePerWorker`: This config decides what's the maximum amount of input data in bytes a single worker can work upon. By default this value is equal to 0 which means that this config is disabled and the worker tries to merge all the data in one shot. Also note that it's imperative that the user set this config if `NumPersisterWorkers > 1`.

If the index is tuned to have a higher `NumPersisterWorkers` value, the memory can potentially drain out faster and ensure stronger consistency behaviour — but there would be a lot of on-disk files, and the background merger would experience the pressure of managing this large number of files, which can be resource-intensive. 
 - Tuning this config is very dependent on the available CPU resources, and something to keep in mind here is that the process's RSS can increase if the number of workers — and each of them working upon a large amount of data — is high.

Increasing the `MaxSizeInMemoryMergePerWorker` value would mean that each worker acts upon a larger amount of data and spends more time merging and flushing it out to disk — which can be healthy behaviour in terms of I/O, although it comes at the cost of time. 
- Changing this config is usecase dependent, for example in usecases where the payload or per doc size is generally large in size (for eg vector usecases), it would be beneficial to have a larger value for this. 

So, having the ideal values for these two configs is definitely dependent on the use case and can involve a bunch of experiments, keeping the resource usage in mind. 


## File Management

The persister introducing some number of file segments into the system would change the state of the system, and the merger would wake up and try to manage these on-disk files. 

Management of these files is crucial when it comes to query latency because a higher number of files would dictate searching through a larger number of files and also higher read amplification to some extent, because the backing data structures can potentially be compacted in size across files. 

The merger sees the files on disk and plans out which segments to merge so that the final layout of segment tiers (each tier having multiple files), which grow in a logarithmic way (the chances of larger tiers growing in number would decrease), is maintained. This also implies that deciding this first-tier size becomes important in deciding the number of segment files across all tiers. 

Starting with v2.5.0, this first-tier size is dependent on the file size using the `FloorSegmentFileSize` config, because that's a better metric to consider (unlike the legacy live doc count metric) in order to ensure that the behaviour is in line with the use case and aware of the payload/doc size. 
- This config can also be tuned to dictate how the I/O behaviour should be within an index. While tuning this config, it should be in proportion to the `MaxSizeInMemoryMergePerWorker` since that dictates the amount of data flushed out per flush. 
- The observation here is that `FloorSegmentFileSize` is lesser than `MaxSizeInMemoryMergePerWorker` and for an optimal I/O during indexing, this value can be set close to `MaxSizeInMemoryMergePerWorker/6`.


## Setting a Persister/Merger Config in Index

The configs are set via the `kvConfig` parameter in the `NewUsing()` or `OpenUsing()` API:

```go
    // setting the persister and merger configs
	kvConfig := map[string]interface{}{
		"scorchPersisterOptions": map[string]interface{}{
			"NumPersisterWorkers":           4,
			"MaxSizeInMemoryMergePerWorker": 20000000,
		},
		"scorchMergePlanOptions": map[string]interface{}{
			"FloorSegmentFileSize": 10000000,
		},
	}
	// passing the config to the index
	index, err := bleve.NewUsing("example.bleve", bleve.NewIndexMapping(), bleve.Config.DefaultIndexType, bleve.Config.DefaultMemKVStore, kvConfig)
	if err != nil {
		panic(err)
	}
```
