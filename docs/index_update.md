# Ability to reduce downtime during index mapping updates

* *v2.5.0* (and after) will come with support to delete certain fields or parts of the fields without requiring a full rebuild of the index
* We do this by storing which portions of the field has to be deleted within zap and then lazily executing the deletion during subsequent merging of the segments

## Usage

While opening an index, if an updated mapping is provided as a string under the key `updated_mapping` within the `runtimeConfig` parameter of `openIndexUsing`, then we open the index and try to update it to use the new mapping provided.

On failure, we still return a usable index with an error explaining why the update failed.

## What can be deleted and what can't be deleted?

* Non updatable changes
    * Any additional fields or enabled document mappings in the new index mapping
    * Any changes to IncludeInAll, type, IncludeTermvECTORS AND SkipFreqNorm
    * Any document mapping having it's enabled value changing from false to true
    * Text fields with a different analyser or date time fields with a different date time format
    * Vector and VectorBase64 fields changing dims, similarity or vectorIndexOptimizedFor
    * Any changes when field is part of `_all`
    * Full field deletions when it is covered by any dynamic setting (Index, Store or DocValues Dynamic)
    * Any changes to dynamic settings at the top level or any enabled document mapping
    * If multiple fields sharing the same field name either from different type mappings or aliases are present, then any non compatible changes across all of these fields
* Updatable changes provided non of the other contitions are hit
    * Index, DocValues, Store of a field changing from true to false
    * Document mapping being disabled or completely removed

## How to enforce immediate deletion?
Since the deletion is only done during merging, a [force merge](https://github.com/blevesearch/bleve/blob/b82baf10b205511cf12da5cb24330abd9f5b1b74/index/scorch/merge.go#L164) may be used to completely remove the stale data.