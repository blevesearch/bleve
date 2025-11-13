//  Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"math"
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeAggregationsBuilder int
var reflectStaticSizeAggregationResult int

func init() {
	var ab AggregationsBuilder
	reflectStaticSizeAggregationsBuilder = int(reflect.TypeOf(ab).Size())
	var ar AggregationResult
	reflectStaticSizeAggregationResult = int(reflect.TypeOf(ar).Size())
}

// AggregationBuilder is the interface all aggregation builders must implement
type AggregationBuilder interface {
	StartDoc()
	UpdateVisitor(field string, term []byte)
	EndDoc()

	Result() *AggregationResult
	Field() string
	Type() string

	Size() int
	Clone() AggregationBuilder // Creates a fresh instance for sub-aggregation bucket cloning
}

// AggregationsBuilder manages multiple aggregation builders
type AggregationsBuilder struct {
	indexReader      index.IndexReader
	aggregationNames []string
	aggregations     []AggregationBuilder
	aggregationsByField map[string][]AggregationBuilder
	fields           []string
}

// NewAggregationsBuilder creates a new aggregations builder
func NewAggregationsBuilder(indexReader index.IndexReader) *AggregationsBuilder {
	return &AggregationsBuilder{
		indexReader: indexReader,
	}
}

func (ab *AggregationsBuilder) Size() int {
	sizeInBytes := reflectStaticSizeAggregationsBuilder + size.SizeOfPtr

	for k, v := range ab.aggregations {
		sizeInBytes += size.SizeOfString + v.Size() + len(ab.aggregationNames[k])
	}

	for _, entry := range ab.fields {
		sizeInBytes += size.SizeOfString + len(entry)
	}

	return sizeInBytes
}

// Add adds an aggregation builder
func (ab *AggregationsBuilder) Add(name string, aggregationBuilder AggregationBuilder) {
	if ab.aggregationsByField == nil {
		ab.aggregationsByField = map[string][]AggregationBuilder{}
	}

	ab.aggregationNames = append(ab.aggregationNames, name)
	ab.aggregations = append(ab.aggregations, aggregationBuilder)

	// Track unique fields
	fieldSet := make(map[string]bool)
	for _, f := range ab.fields {
		fieldSet[f] = true
	}

	// Register for the aggregation's own field
	field := aggregationBuilder.Field()
	ab.aggregationsByField[field] = append(ab.aggregationsByField[field], aggregationBuilder)
	if !fieldSet[field] {
		ab.fields = append(ab.fields, field)
		fieldSet[field] = true
	}

	// For bucket aggregations, also register for sub-aggregation fields
	if bucketed, ok := aggregationBuilder.(BucketAggregation); ok {
		subFields := bucketed.SubAggregationFields()
		for _, subField := range subFields {
			ab.aggregationsByField[subField] = append(ab.aggregationsByField[subField], aggregationBuilder)
			if !fieldSet[subField] {
				ab.fields = append(ab.fields, subField)
				fieldSet[subField] = true
			}
		}
	}
}

// BucketAggregation interface for aggregations that have sub-aggregations
type BucketAggregation interface {
	AggregationBuilder
	SubAggregationFields() []string
}

// RequiredFields returns the fields needed for aggregations
func (ab *AggregationsBuilder) RequiredFields() []string {
	return ab.fields
}

// StartDoc notifies all aggregations that a new document is being processed
func (ab *AggregationsBuilder) StartDoc() {
	for _, aggregationBuilder := range ab.aggregations {
		aggregationBuilder.StartDoc()
	}
}

// UpdateVisitor forwards field values to relevant aggregation builders
func (ab *AggregationsBuilder) UpdateVisitor(field string, term []byte) {
	if aggregationBuilders, ok := ab.aggregationsByField[field]; ok {
		for _, aggregationBuilder := range aggregationBuilders {
			aggregationBuilder.UpdateVisitor(field, term)
		}
	}
}

// EndDoc notifies all aggregations that document processing is complete
func (ab *AggregationsBuilder) EndDoc() {
	for _, aggregationBuilder := range ab.aggregations {
		aggregationBuilder.EndDoc()
	}
}

// Results returns all aggregation results
func (ab *AggregationsBuilder) Results() AggregationResults {
	results := make(AggregationResults, len(ab.aggregations))
	for i, aggregationBuilder := range ab.aggregations {
		results[ab.aggregationNames[i]] = aggregationBuilder.Result()
	}
	return results
}

// AggregationResult represents the result of an aggregation
// For metric aggregations, Value contains a single number (float64 or int64)
// For bucket aggregations, Value contains a slice of *Bucket
type AggregationResult struct {
	Field string      `json:"field"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`

	// For bucket aggregations only
	Buckets []*Bucket `json:"buckets,omitempty"`
}

// StatsResult contains comprehensive statistics
type StatsResult struct {
	Count      int64   `json:"count"`
	Sum        float64 `json:"sum"`
	Avg        float64 `json:"avg"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
	SumSquares float64 `json:"sum_squares"`
	Variance   float64 `json:"variance"`
	StdDev     float64 `json:"std_dev"`
}

// Bucket represents a single bucket in a bucket aggregation
type Bucket struct {
	Key          interface{}                  `json:"key"`           // Term or range name
	Count        int64                        `json:"doc_count"`     // Number of documents in this bucket
	Aggregations map[string]*AggregationResult `json:"aggregations,omitempty"` // Sub-aggregations
}

func (ar *AggregationResult) Size() int {
	sizeInBytes := reflectStaticSizeAggregationResult
	sizeInBytes += len(ar.Field)
	sizeInBytes += len(ar.Type)
	// Value size depends on type, using approximate size
	sizeInBytes += size.SizeOfFloat64

	// Add bucket sizes
	for _, bucket := range ar.Buckets {
		sizeInBytes += size.SizeOfPtr + 8 // int64 count = 8 bytes
		// Approximate size for key
		sizeInBytes += size.SizeOfString + 20
		// Approximate size for sub-aggregations
		for _, subAgg := range bucket.Aggregations {
			sizeInBytes += subAgg.Size()
		}
	}

	return sizeInBytes
}

// AggregationResults is a map of aggregation results by name
type AggregationResults map[string]*AggregationResult

// Merge merges another set of aggregation results into this one
// This is useful for combining results from multiple index shards
// Note: avg merging is approximate without storing counts separately
func (ar AggregationResults) Merge(other AggregationResults) {
	for name, otherAggResult := range other {
		aggResult, exists := ar[name]
		if !exists {
			// First time seeing this aggregation, just copy it
			ar[name] = otherAggResult
			continue
		}

		// Merge based on aggregation type
		switch aggResult.Type {
		case "sum", "sumsquares":
			// Sum values are additive
			aggResult.Value = aggResult.Value.(float64) + otherAggResult.Value.(float64)

		case "count":
			// Counts are additive
			aggResult.Value = aggResult.Value.(int64) + otherAggResult.Value.(int64)

		case "min":
			// Take minimum of minimums
			if otherAggResult.Value.(float64) < aggResult.Value.(float64) {
				aggResult.Value = otherAggResult.Value
			}

		case "max":
			// Take maximum of maximums
			if otherAggResult.Value.(float64) > aggResult.Value.(float64) {
				aggResult.Value = otherAggResult.Value
			}

		case "avg":
			// Average of averages is approximate - proper merging requires counts
			// For now, take simple average (limitation)
			aggResult.Value = (aggResult.Value.(float64) + otherAggResult.Value.(float64)) / 2.0

		case "stats":
			// Merge stats by combining component values
			destStats := aggResult.Value.(*StatsResult)
			srcStats := otherAggResult.Value.(*StatsResult)

			destStats.Count += srcStats.Count
			destStats.Sum += srcStats.Sum
			destStats.SumSquares += srcStats.SumSquares

			if srcStats.Min < destStats.Min {
				destStats.Min = srcStats.Min
			}
			if srcStats.Max > destStats.Max {
				destStats.Max = srcStats.Max
			}

			// Recalculate derived values
			if destStats.Count > 0 {
				destStats.Avg = destStats.Sum / float64(destStats.Count)
				avgSquares := destStats.SumSquares / float64(destStats.Count)
				destStats.Variance = avgSquares - (destStats.Avg * destStats.Avg)
				if destStats.Variance < 0 {
					destStats.Variance = 0
				}
				destStats.StdDev = math.Sqrt(destStats.Variance)
			}

		case "terms", "range", "date_range":
			// Merge buckets
			ar.mergeBuckets(aggResult, otherAggResult)
		}
	}
}

// mergeBuckets merges bucket aggregation results
func (ar AggregationResults) mergeBuckets(dest, src *AggregationResult) {
	// Create a map of existing buckets by key
	bucketMap := make(map[interface{}]*Bucket)
	for _, bucket := range dest.Buckets {
		bucketMap[bucket.Key] = bucket
	}

	// Merge source buckets
	for _, srcBucket := range src.Buckets {
		destBucket, exists := bucketMap[srcBucket.Key]
		if !exists {
			// New bucket, add it
			dest.Buckets = append(dest.Buckets, srcBucket)
			bucketMap[srcBucket.Key] = srcBucket
		} else {
			// Existing bucket, merge counts
			destBucket.Count += srcBucket.Count

			// Merge sub-aggregations recursively
			if srcBucket.Aggregations != nil {
				if destBucket.Aggregations == nil {
					destBucket.Aggregations = make(map[string]*AggregationResult)
				}
				AggregationResults(destBucket.Aggregations).Merge(srcBucket.Aggregations)
			}
		}
	}
}
