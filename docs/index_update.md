# Ability to reduce downtime during index mapping updates

* *v2.5.4* (and after) will come with support to delete or modify any field mapping in the index mapping without requiring a full rebuild of the index
* We do this by storing which portions of the field has to be deleted within zap and then lazily executing the deletion during subsequent merging of the segments

## Usage

While opening an index, if an updated mapping is provided as a string under the key `updated_mapping` within the `runtimeConfig` parameter of `OpenUsing`, then we open the index and try to update it to use the new mapping provided.

If the update fails, the index is unchanged and an error is returned explaining why the update was unsuccessful.

## What can be deleted and what can't be deleted?
Fields can be partially deleted by changing their Index, Store, and DocValues parameters from true to false, or completely removed by deleting the field itself.

Additionally, document mappings can be deleted either by fully removing them from the index mapping or by setting the Enabled value to false, which deletes all fields defined within that mapping.

However, if any of the following conditions are met, the index is considered non-updatable.
* Any additional fields or enabled document mappings in the new index mapping
* Any changes to IncludeInAll, type, IncludeTermVectors and SkipFreqNorm
* Any document mapping having it's enabled value changing from false to true
* Text fields with a different analyser or date time fields with a different date time format
* Vector and VectorBase64 fields changing dims, similarity or vectorIndexOptimizedFor
* Any changes when field is part of `_all`
* Full field deletions when it is covered by any dynamic setting (Index, Store or DocValues Dynamic)
* Any changes to dynamic settings at the top level or any enabled document mapping
* If multiple fields sharing the same field name either from different type mappings or aliases are present, then any non compatible changes across all of these fields

## How to enforce immediate deletion?
Since the deletion is only done during merging, a [force merge](https://github.com/blevesearch/bleve/blob/b82baf10b205511cf12da5cb24330abd9f5b1b74/index/scorch/merge.go#L164) may be used to completely remove the stale data.

## Sample code to update an existing index
```
newMapping := `<Updated Index Mapping>`
config := map[string]interface{}{
    "updated_mapping": newMapping
}
index, err := OpenUsing("<Path to Index>", config)
if err != nil {
    return err
}
```
