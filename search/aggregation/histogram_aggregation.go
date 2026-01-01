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
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var (
	reflectStaticSizeHistogramAggregation     int
	reflectStaticSizeDateHistogramAggregation int
)

func init() {
	var ha HistogramAggregation
	reflectStaticSizeHistogramAggregation = int(reflect.TypeOf(ha).Size())
	var dha DateHistogramAggregation
	reflectStaticSizeDateHistogramAggregation = int(reflect.TypeOf(dha).Size())
}

// HistogramAggregation groups numeric values into fixed-interval buckets
type HistogramAggregation struct {
	field          string
	interval       float64 // Bucket interval (e.g., 100 for price buckets every $100)
	minDocCount    int64   // Minimum document count to include bucket (default 0)
	bucketCounts   map[float64]int64                // bucket key -> document count
	bucketSubAggs  map[float64]*subAggregationSet   // bucket key -> sub-aggregations
	subAggBuilders map[string]search.AggregationBuilder
	currentBucket  float64
	sawValue       bool
}

// NewHistogramAggregation creates a new histogram aggregation
func NewHistogramAggregation(field string, interval float64, minDocCount int64, subAggregations map[string]search.AggregationBuilder) *HistogramAggregation {
	if interval <= 0 {
		interval = 1.0 // default interval
	}
	if minDocCount < 0 {
		minDocCount = 0
	}

	return &HistogramAggregation{
		field:          field,
		interval:       interval,
		minDocCount:    minDocCount,
		bucketCounts:   make(map[float64]int64),
		bucketSubAggs:  make(map[float64]*subAggregationSet),
		subAggBuilders: subAggregations,
	}
}

func (ha *HistogramAggregation) Size() int {
	sizeInBytes := reflectStaticSizeHistogramAggregation + size.SizeOfPtr +
		len(ha.field)

	for range ha.bucketCounts {
		sizeInBytes += size.SizeOfFloat64 + 8 // key + int64 count
	}
	return sizeInBytes
}

func (ha *HistogramAggregation) Field() string {
	return ha.field
}

func (ha *HistogramAggregation) Type() string {
	return "histogram"
}

func (ha *HistogramAggregation) SubAggregationFields() []string {
	if ha.subAggBuilders == nil {
		return nil
	}
	fieldSet := make(map[string]bool)
	for _, subAgg := range ha.subAggBuilders {
		fieldSet[subAgg.Field()] = true
		if bucketed, ok := subAgg.(search.BucketAggregation); ok {
			for _, f := range bucketed.SubAggregationFields() {
				fieldSet[f] = true
			}
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	return fields
}

func (ha *HistogramAggregation) StartDoc() {
	ha.sawValue = false
	ha.currentBucket = 0
}

func (ha *HistogramAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, compute bucket key
	if field == ha.field {
		if !ha.sawValue {
			ha.sawValue = true

			// Decode numeric value
			prefixCoded := numeric.PrefixCoded(term)
			shift, err := prefixCoded.Shift()
			if err == nil && shift == 0 {
				i64, err := prefixCoded.Int64()
				if err == nil {
					f64 := numeric.Int64ToFloat64(i64)

					// Calculate bucket key by rounding down to nearest interval
					bucketKey := math.Floor(f64/ha.interval) * ha.interval
					ha.currentBucket = bucketKey

					// Increment count for this bucket
					ha.bucketCounts[bucketKey]++

					// Initialize sub-aggregations for this bucket if needed
					if ha.subAggBuilders != nil && len(ha.subAggBuilders) > 0 {
						if _, exists := ha.bucketSubAggs[bucketKey]; !exists {
							ha.bucketSubAggs[bucketKey] = &subAggregationSet{
								builders: ha.cloneSubAggBuilders(),
							}
						}
						// Start document processing for this bucket's sub-aggregations
						if subAggs, exists := ha.bucketSubAggs[bucketKey]; exists {
							for _, subAgg := range subAggs.builders {
								subAgg.StartDoc()
							}
						}
					}
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current bucket
	if ha.sawValue && ha.subAggBuilders != nil {
		if subAggs, exists := ha.bucketSubAggs[ha.currentBucket]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.UpdateVisitor(field, term)
			}
		}
	}
}

func (ha *HistogramAggregation) EndDoc() {
	if ha.sawValue && ha.subAggBuilders != nil {
		// End document for all sub-aggregations in this bucket
		if subAggs, exists := ha.bucketSubAggs[ha.currentBucket]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.EndDoc()
			}
		}
	}
}

func (ha *HistogramAggregation) Result() *search.AggregationResult {
	// Collect buckets that meet minDocCount
	type bucketInfo struct {
		key   float64
		count int64
	}

	buckets := make([]bucketInfo, 0, len(ha.bucketCounts))
	for key, count := range ha.bucketCounts {
		if count >= ha.minDocCount {
			buckets = append(buckets, bucketInfo{key, count})
		}
	}

	// Sort buckets by key (ascending)
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].key < buckets[j].key
	})

	// Build bucket results with sub-aggregations
	resultBuckets := make([]*search.Bucket, len(buckets))
	for i, b := range buckets {
		bucket := &search.Bucket{
			Key:   b.key,
			Count: b.count,
		}

		// Add sub-aggregation results for this bucket
		if subAggs, exists := ha.bucketSubAggs[b.key]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		resultBuckets[i] = bucket
	}

	return &search.AggregationResult{
		Field:   ha.field,
		Type:    "histogram",
		Buckets: resultBuckets,
		Metadata: map[string]interface{}{
			"interval": ha.interval,
		},
	}
}

func (ha *HistogramAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if ha.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(ha.subAggBuilders))
		for name, subAgg := range ha.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	return NewHistogramAggregation(ha.field, ha.interval, ha.minDocCount, clonedSubAggs)
}

func (ha *HistogramAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(ha.subAggBuilders))
	for name, builder := range ha.subAggBuilders {
		cloned[name] = builder.Clone()
	}
	return cloned
}

// DateHistogramAggregation groups datetime values into fixed-interval buckets
type DateHistogramAggregation struct {
	field            string
	calendarInterval CalendarInterval // Calendar interval (e.g., "1d", "1M")
	fixedInterval    *time.Duration   // Fixed duration interval (alternative to calendar)
	minDocCount      int64            // Minimum document count to include bucket
	bucketCounts     map[int64]int64                 // bucket timestamp -> document count
	bucketSubAggs    map[int64]*subAggregationSet    // bucket timestamp -> sub-aggregations
	subAggBuilders   map[string]search.AggregationBuilder
	currentBucket    int64
	sawValue         bool
}

// CalendarInterval represents calendar-aware intervals (day, month, year, etc.)
type CalendarInterval string

const (
	CalendarIntervalMinute  CalendarInterval = "1m"
	CalendarIntervalHour    CalendarInterval = "1h"
	CalendarIntervalDay     CalendarInterval = "1d"
	CalendarIntervalWeek    CalendarInterval = "1w"
	CalendarIntervalMonth   CalendarInterval = "1M"
	CalendarIntervalQuarter CalendarInterval = "1q"
	CalendarIntervalYear    CalendarInterval = "1y"
)

// NewDateHistogramAggregation creates a new date histogram aggregation with calendar interval
func NewDateHistogramAggregation(field string, calendarInterval CalendarInterval, minDocCount int64, subAggregations map[string]search.AggregationBuilder) *DateHistogramAggregation {
	if minDocCount < 0 {
		minDocCount = 0
	}

	return &DateHistogramAggregation{
		field:            field,
		calendarInterval: calendarInterval,
		minDocCount:      minDocCount,
		bucketCounts:     make(map[int64]int64),
		bucketSubAggs:    make(map[int64]*subAggregationSet),
		subAggBuilders:   subAggregations,
	}
}

// NewDateHistogramAggregationWithFixedInterval creates a new date histogram with fixed duration
func NewDateHistogramAggregationWithFixedInterval(field string, interval time.Duration, minDocCount int64, subAggregations map[string]search.AggregationBuilder) *DateHistogramAggregation {
	if minDocCount < 0 {
		minDocCount = 0
	}

	return &DateHistogramAggregation{
		field:          field,
		fixedInterval:  &interval,
		minDocCount:    minDocCount,
		bucketCounts:   make(map[int64]int64),
		bucketSubAggs:  make(map[int64]*subAggregationSet),
		subAggBuilders: subAggregations,
	}
}

func (dha *DateHistogramAggregation) Size() int {
	sizeInBytes := reflectStaticSizeDateHistogramAggregation + size.SizeOfPtr +
		len(dha.field)

	for range dha.bucketCounts {
		sizeInBytes += 8 + 8 // int64 key + int64 count
	}
	return sizeInBytes
}

func (dha *DateHistogramAggregation) Field() string {
	return dha.field
}

func (dha *DateHistogramAggregation) Type() string {
	return "date_histogram"
}

func (dha *DateHistogramAggregation) SubAggregationFields() []string {
	if dha.subAggBuilders == nil {
		return nil
	}
	fieldSet := make(map[string]bool)
	for _, subAgg := range dha.subAggBuilders {
		fieldSet[subAgg.Field()] = true
		if bucketed, ok := subAgg.(search.BucketAggregation); ok {
			for _, f := range bucketed.SubAggregationFields() {
				fieldSet[f] = true
			}
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	return fields
}

func (dha *DateHistogramAggregation) StartDoc() {
	dha.sawValue = false
	dha.currentBucket = 0
}

func (dha *DateHistogramAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, compute bucket timestamp
	if field == dha.field {
		if !dha.sawValue {
			dha.sawValue = true

			// Decode datetime value (stored as nanoseconds since epoch)
			prefixCoded := numeric.PrefixCoded(term)
			shift, err := prefixCoded.Shift()
			if err == nil && shift == 0 {
				i64, err := prefixCoded.Int64()
				if err == nil {
					t := time.Unix(0, i64).UTC()

					// Calculate bucket key by rounding down to interval boundary
					var bucketKey int64
					if dha.fixedInterval != nil {
						// Fixed interval: round down to nearest interval
						nanos := t.UnixNano()
						intervalNanos := dha.fixedInterval.Nanoseconds()
						bucketKey = (nanos / intervalNanos) * intervalNanos
					} else {
						// Calendar interval: use calendar-aware rounding
						bucketKey = dha.roundToCalendarInterval(t).UnixNano()
					}

					dha.currentBucket = bucketKey

					// Increment count for this bucket
					dha.bucketCounts[bucketKey]++

					// Initialize sub-aggregations for this bucket if needed
					if dha.subAggBuilders != nil && len(dha.subAggBuilders) > 0 {
						if _, exists := dha.bucketSubAggs[bucketKey]; !exists {
							dha.bucketSubAggs[bucketKey] = &subAggregationSet{
								builders: dha.cloneSubAggBuilders(),
							}
						}
						// Start document processing for this bucket's sub-aggregations
						if subAggs, exists := dha.bucketSubAggs[bucketKey]; exists {
							for _, subAgg := range subAggs.builders {
								subAgg.StartDoc()
							}
						}
					}
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current bucket
	if dha.sawValue && dha.subAggBuilders != nil {
		if subAggs, exists := dha.bucketSubAggs[dha.currentBucket]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.UpdateVisitor(field, term)
			}
		}
	}
}

func (dha *DateHistogramAggregation) EndDoc() {
	if dha.sawValue && dha.subAggBuilders != nil {
		// End document for all sub-aggregations in this bucket
		if subAggs, exists := dha.bucketSubAggs[dha.currentBucket]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.EndDoc()
			}
		}
	}
}

func (dha *DateHistogramAggregation) Result() *search.AggregationResult {
	// Collect buckets that meet minDocCount
	type bucketInfo struct {
		key   int64
		count int64
	}

	buckets := make([]bucketInfo, 0, len(dha.bucketCounts))
	for key, count := range dha.bucketCounts {
		if count >= dha.minDocCount {
			buckets = append(buckets, bucketInfo{key, count})
		}
	}

	// Sort buckets by timestamp (ascending)
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].key < buckets[j].key
	})

	// Build bucket results with sub-aggregations
	resultBuckets := make([]*search.Bucket, len(buckets))
	for i, b := range buckets {
		// Convert timestamp to ISO 8601 string for the key
		bucketTime := time.Unix(0, b.key).UTC()
		bucket := &search.Bucket{
			Key:   bucketTime.Format(time.RFC3339),
			Count: b.count,
			Metadata: map[string]interface{}{
				"timestamp": b.key, // Keep numeric timestamp for reference
			},
		}

		// Add sub-aggregation results for this bucket
		if subAggs, exists := dha.bucketSubAggs[b.key]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		resultBuckets[i] = bucket
	}

	metadata := map[string]interface{}{}
	if dha.fixedInterval != nil {
		metadata["interval"] = dha.fixedInterval.String()
	} else {
		metadata["calendar_interval"] = string(dha.calendarInterval)
	}

	return &search.AggregationResult{
		Field:    dha.field,
		Type:     "date_histogram",
		Buckets:  resultBuckets,
		Metadata: metadata,
	}
}

func (dha *DateHistogramAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if dha.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(dha.subAggBuilders))
		for name, subAgg := range dha.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	if dha.fixedInterval != nil {
		return NewDateHistogramAggregationWithFixedInterval(dha.field, *dha.fixedInterval, dha.minDocCount, clonedSubAggs)
	}
	return NewDateHistogramAggregation(dha.field, dha.calendarInterval, dha.minDocCount, clonedSubAggs)
}

func (dha *DateHistogramAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(dha.subAggBuilders))
	for name, builder := range dha.subAggBuilders {
		cloned[name] = builder.Clone()
	}
	return cloned
}

// roundToCalendarInterval rounds a time down to the nearest calendar interval boundary
func (dha *DateHistogramAggregation) roundToCalendarInterval(t time.Time) time.Time {
	switch dha.calendarInterval {
	case CalendarIntervalMinute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case CalendarIntervalHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case CalendarIntervalDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case CalendarIntervalWeek:
		// Round to start of week (Monday)
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7 // Sunday -> 7
		}
		daysBack := weekday - 1
		return time.Date(t.Year(), t.Month(), t.Day()-daysBack, 0, 0, 0, 0, t.Location())
	case CalendarIntervalMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case CalendarIntervalQuarter:
		// Round to start of quarter (Jan 1, Apr 1, Jul 1, Oct 1)
		month := t.Month()
		quarterStartMonth := ((month-1)/3)*3 + 1
		return time.Date(t.Year(), quarterStartMonth, 1, 0, 0, 0, 0, t.Location())
	case CalendarIntervalYear:
		return time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location())
	default:
		// Default to day
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}
}
