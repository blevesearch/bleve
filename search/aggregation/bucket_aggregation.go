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

package aggregation

import (
	"bytes"
	"reflect"
	"regexp"
	"sort"

	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var (
	reflectStaticSizeTermsAggregation  int
	reflectStaticSizeRangeAggregation  int
)

func init() {
	var ta TermsAggregation
	reflectStaticSizeTermsAggregation = int(reflect.TypeOf(ta).Size())
	var ra RangeAggregation
	reflectStaticSizeRangeAggregation = int(reflect.TypeOf(ra).Size())
}

// TermsAggregation groups documents by unique field values
type TermsAggregation struct {
	field          string
	size           int
	prefixBytes    []byte                        // Pre-converted prefix for fast matching
	regex          *regexp.Regexp                // Pre-compiled regex for pattern matching
	termCounts     map[string]int64              // term -> document count
	termSubAggs    map[string]*subAggregationSet // term -> sub-aggregations
	subAggBuilders map[string]search.AggregationBuilder
	currentTerm    string
	sawValue       bool
}

// subAggregationSet holds the set of sub-aggregations for a bucket
type subAggregationSet struct {
	builders map[string]search.AggregationBuilder
}

// NewTermsAggregation creates a new terms aggregation
func NewTermsAggregation(field string, size int, subAggregations map[string]search.AggregationBuilder) *TermsAggregation {
	if size <= 0 {
		size = 10 // default
	}

	return &TermsAggregation{
		field:          field,
		size:           size,
		termCounts:     make(map[string]int64),
		termSubAggs:    make(map[string]*subAggregationSet),
		subAggBuilders: subAggregations,
	}
}

func (ta *TermsAggregation) Size() int {
	sizeInBytes := reflectStaticSizeTermsAggregation + size.SizeOfPtr +
		len(ta.field) +
		len(ta.prefixBytes) +
		size.SizeOfPtr // regex pointer (does not include actual regexp.Regexp object size)

	// Estimate regex object size if present.
	if ta.regex != nil {
		// This is only the static size of regexp.Regexp struct, not including heap allocations.
		sizeInBytes += int(reflect.TypeOf(*ta.regex).Size())
		// NOTE: Actual memory usage of regexp.Regexp may be higher due to internal allocations.
	}

	for term := range ta.termCounts {
		sizeInBytes += size.SizeOfString + len(term) + 8 // int64 = 8 bytes
	}
	return sizeInBytes
}

func (ta *TermsAggregation) Field() string {
	return ta.field
}

// SetPrefixFilter sets the prefix filter for term aggregations.
func (ta *TermsAggregation) SetPrefixFilter(prefix string) {
	if prefix != "" {
		ta.prefixBytes = []byte(prefix)
	} else {
		ta.prefixBytes = nil
	}
}

// SetRegexFilter sets the compiled regex filter for term aggregations.
func (ta *TermsAggregation) SetRegexFilter(regex *regexp.Regexp) {
	ta.regex = regex
}

func (ta *TermsAggregation) Type() string {
	return "terms"
}

func (ta *TermsAggregation) SubAggregationFields() []string {
	if ta.subAggBuilders == nil {
		return nil
	}
	// Use a map to track unique fields
	fieldSet := make(map[string]bool)
	for _, subAgg := range ta.subAggBuilders {
		fieldSet[subAgg.Field()] = true
		// If sub-agg is also a bucket, recursively collect its fields
		if bucketed, ok := subAgg.(search.BucketAggregation); ok {
			for _, f := range bucketed.SubAggregationFields() {
				fieldSet[f] = true
			}
		}
	}
	// Convert map to slice
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	return fields
}

func (ta *TermsAggregation) StartDoc() {
	ta.sawValue = false
	ta.currentTerm = ""
}

func (ta *TermsAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, track the bucket
	if field == ta.field {
		// Fast prefix check on []byte - zero allocation
		if len(ta.prefixBytes) > 0 && !bytes.HasPrefix(term, ta.prefixBytes) {
			return // Skip terms that don't match prefix
		}

		// Fast regex check on []byte - zero allocation
		if ta.regex != nil && !ta.regex.Match(term) {
			return // Skip terms that don't match regex
		}

		ta.sawValue = true
		// Only convert to string if term matches filters
		termStr := string(term)
		ta.currentTerm = termStr

		// Increment count for this term
		ta.termCounts[termStr]++

		// Initialize sub-aggregations for this term if needed
		if ta.subAggBuilders != nil && len(ta.subAggBuilders) > 0 {
			if _, exists := ta.termSubAggs[termStr]; !exists {
				// Clone sub-aggregation builders for this bucket
				ta.termSubAggs[termStr] = &subAggregationSet{
					builders: ta.cloneSubAggBuilders(),
				}
			}
			// Start document processing for this bucket's sub-aggregations
			// This is called once per document for the bucket it falls into
			if subAggs, exists := ta.termSubAggs[termStr]; exists {
				for _, subAgg := range subAggs.builders {
					subAgg.StartDoc()
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current bucket
	if ta.currentTerm != "" && ta.subAggBuilders != nil {
		if subAggs, exists := ta.termSubAggs[ta.currentTerm]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.UpdateVisitor(field, term)
			}
		}
	}
}

func (ta *TermsAggregation) EndDoc() {
	if ta.sawValue && ta.currentTerm != "" && ta.subAggBuilders != nil {
		// End document for all sub-aggregations in this bucket
		if subAggs, exists := ta.termSubAggs[ta.currentTerm]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.EndDoc()
			}
		}
	}
}

func (ta *TermsAggregation) Result() *search.AggregationResult {
	// Sort terms by count (descending) and take top N
	type termCount struct {
		term  string
		count int64
	}

	terms := make([]termCount, 0, len(ta.termCounts))
	for term, count := range ta.termCounts {
		terms = append(terms, termCount{term, count})
	}

	sort.Slice(terms, func(i, j int) bool {
		return terms[i].count > terms[j].count
	})

	// Limit to size
	if len(terms) > ta.size {
		terms = terms[:ta.size]
	}

	// Build buckets with sub-aggregation results
	buckets := make([]*search.Bucket, len(terms))
	for i, tc := range terms {
		bucket := &search.Bucket{
			Key:   tc.term,
			Count: tc.count,
		}

		// Add sub-aggregation results for this bucket
		if subAggs, exists := ta.termSubAggs[tc.term]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		buckets[i] = bucket
	}

	return &search.AggregationResult{
		Field:   ta.field,
		Type:    "terms",
		Buckets: buckets,
	}
}

func (ta *TermsAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if ta.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(ta.subAggBuilders))
		for name, subAgg := range ta.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	// Create new terms aggregation
	cloned := NewTermsAggregation(ta.field, ta.size, clonedSubAggs)

	// Copy filters
	if ta.prefixBytes != nil {
		cloned.prefixBytes = make([]byte, len(ta.prefixBytes))
		copy(cloned.prefixBytes, ta.prefixBytes)
	}
	if ta.regex != nil {
		cloned.regex = ta.regex // regexp.Regexp is safe to share
	}

	return cloned
}

// cloneSubAggBuilders creates fresh instances of sub-aggregation builders
func (ta *TermsAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(ta.subAggBuilders))
	for name, builder := range ta.subAggBuilders {
		// Use Clone() method which properly handles all aggregation types including nested buckets
		cloned[name] = builder.Clone()
	}
	return cloned
}

// RangeAggregation groups documents into numeric ranges
type RangeAggregation struct {
	field          string
	ranges         map[string]*NumericRange
	rangeCounts    map[string]int64
	rangeSubAggs   map[string]*subAggregationSet
	subAggBuilders map[string]search.AggregationBuilder
	currentRanges  []string // ranges the current value falls into
	sawValue       bool
}

// NumericRange represents a numeric range for range aggregations
type NumericRange struct {
	Name string
	Min  *float64
	Max  *float64
}

// NewRangeAggregation creates a new range aggregation
func NewRangeAggregation(field string, ranges map[string]*NumericRange, subAggregations map[string]search.AggregationBuilder) *RangeAggregation {
	return &RangeAggregation{
		field:          field,
		ranges:         ranges,
		rangeCounts:    make(map[string]int64),
		rangeSubAggs:   make(map[string]*subAggregationSet),
		subAggBuilders: subAggregations,
		currentRanges:  make([]string, 0, len(ranges)),
	}
}

func (ra *RangeAggregation) Size() int {
	return reflectStaticSizeRangeAggregation + size.SizeOfPtr + len(ra.field)
}

func (ra *RangeAggregation) Field() string {
	return ra.field
}

func (ra *RangeAggregation) Type() string {
	return "range"
}

func (ra *RangeAggregation) SubAggregationFields() []string {
	if ra.subAggBuilders == nil {
		return nil
	}
	// Use a map to track unique fields
	fieldSet := make(map[string]bool)
	for _, subAgg := range ra.subAggBuilders {
		fieldSet[subAgg.Field()] = true
		// If sub-agg is also a bucket, recursively collect its fields
		if bucketed, ok := subAgg.(search.BucketAggregation); ok {
			for _, f := range bucketed.SubAggregationFields() {
				fieldSet[f] = true
			}
		}
	}
	// Convert map to slice
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	return fields
}

func (ra *RangeAggregation) StartDoc() {
	ra.sawValue = false
	ra.currentRanges = ra.currentRanges[:0]
}

func (ra *RangeAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, determine which ranges this document falls into
	if field == ra.field {
		ra.sawValue = true

		// Decode numeric value
		prefixCoded := numeric.PrefixCoded(term)
		shift, err := prefixCoded.Shift()
		if err == nil && shift == 0 {
			i64, err := prefixCoded.Int64()
			if err == nil {
				f64 := numeric.Int64ToFloat64(i64)

				// Check which ranges this value falls into
				for rangeName, r := range ra.ranges {
					if (r.Min == nil || f64 >= *r.Min) && (r.Max == nil || f64 < *r.Max) {
						ra.rangeCounts[rangeName]++
						ra.currentRanges = append(ra.currentRanges, rangeName)

						// Initialize sub-aggregations for this range if needed
						if ra.subAggBuilders != nil && len(ra.subAggBuilders) > 0 {
							if _, exists := ra.rangeSubAggs[rangeName]; !exists {
								ra.rangeSubAggs[rangeName] = &subAggregationSet{
									builders: ra.cloneSubAggBuilders(),
								}
							}
						}
					}
				}

				// Start document processing for all ranges this document falls into
				// This is called once per document for each range it falls into
				if ra.subAggBuilders != nil && len(ra.subAggBuilders) > 0 {
					for _, rangeName := range ra.currentRanges {
						if subAggs, exists := ra.rangeSubAggs[rangeName]; exists {
							for _, subAgg := range subAggs.builders {
								subAgg.StartDoc()
							}
						}
					}
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current ranges
	if ra.subAggBuilders != nil {
		for _, rangeName := range ra.currentRanges {
			if subAggs, exists := ra.rangeSubAggs[rangeName]; exists {
				for _, subAgg := range subAggs.builders {
					subAgg.UpdateVisitor(field, term)
				}
			}
		}
	}
}

func (ra *RangeAggregation) EndDoc() {
	if ra.sawValue && ra.subAggBuilders != nil {
		// End document for all affected ranges
		for _, rangeName := range ra.currentRanges {
			if subAggs, exists := ra.rangeSubAggs[rangeName]; exists {
				for _, subAgg := range subAggs.builders {
					subAgg.EndDoc()
				}
			}
		}
	}
}

func (ra *RangeAggregation) Result() *search.AggregationResult {
	buckets := make([]*search.Bucket, 0, len(ra.ranges))

	for rangeName := range ra.ranges {
		bucket := &search.Bucket{
			Key:   rangeName,
			Count: ra.rangeCounts[rangeName],
		}

		// Add sub-aggregation results
		if subAggs, exists := ra.rangeSubAggs[rangeName]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		buckets = append(buckets, bucket)
	}

	// Sort buckets by key
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Key.(string) < buckets[j].Key.(string)
	})

	return &search.AggregationResult{
		Field:   ra.field,
		Type:    "range",
		Buckets: buckets,
	}
}

func (ra *RangeAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if ra.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(ra.subAggBuilders))
		for name, subAgg := range ra.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	// Deep copy ranges
	clonedRanges := make(map[string]*NumericRange, len(ra.ranges))
	for name, r := range ra.ranges {
		clonedRange := &NumericRange{
			Name: r.Name,
		}
		if r.Min != nil {
			min := *r.Min
			clonedRange.Min = &min
		}
		if r.Max != nil {
			max := *r.Max
			clonedRange.Max = &max
		}
		clonedRanges[name] = clonedRange
	}

	return NewRangeAggregation(ra.field, clonedRanges, clonedSubAggs)
}

func (ra *RangeAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(ra.subAggBuilders))
	for name, builder := range ra.subAggBuilders {
		// Use Clone() method which properly handles all aggregation types including nested buckets
		cloned[name] = builder.Clone()
	}
	return cloned
}
