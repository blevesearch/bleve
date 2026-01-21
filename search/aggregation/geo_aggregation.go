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

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var (
	reflectStaticSizeGeohashGridAggregation int
	reflectStaticSizeGeoDistanceAggregation int
)

func init() {
	var gga GeohashGridAggregation
	reflectStaticSizeGeohashGridAggregation = int(reflect.TypeOf(gga).Size())
	var gda GeoDistanceAggregation
	reflectStaticSizeGeoDistanceAggregation = int(reflect.TypeOf(gda).Size())
}

// GeohashGridAggregation groups geo points by geohash grid cells
type GeohashGridAggregation struct {
	field          string
	precision      int                               // Geohash precision (1-12)
	size           int                               // Max number of buckets to return
	cellCounts     map[string]int64                  // geohash -> document count
	cellSubAggs    map[string]*subAggregationSet     // geohash -> sub-aggregations
	subAggBuilders map[string]search.AggregationBuilder
	currentCell    string
	sawValue       bool
}

// NewGeohashGridAggregation creates a new geohash grid aggregation
func NewGeohashGridAggregation(field string, precision int, size int, subAggregations map[string]search.AggregationBuilder) *GeohashGridAggregation {
	if precision <= 0 || precision > 12 {
		precision = 5 // default: ~5km x 5km cells
	}
	if size <= 0 {
		size = 10 // default
	}

	return &GeohashGridAggregation{
		field:          field,
		precision:      precision,
		size:           size,
		cellCounts:     make(map[string]int64),
		cellSubAggs:    make(map[string]*subAggregationSet),
		subAggBuilders: subAggregations,
	}
}

func (gga *GeohashGridAggregation) Size() int {
	sizeInBytes := reflectStaticSizeGeohashGridAggregation + size.SizeOfPtr +
		len(gga.field)

	for cell := range gga.cellCounts {
		sizeInBytes += size.SizeOfString + len(cell) + 8 // int64 = 8 bytes
	}
	return sizeInBytes
}

func (gga *GeohashGridAggregation) Field() string {
	return gga.field
}

func (gga *GeohashGridAggregation) Type() string {
	return "geohash_grid"
}

func (gga *GeohashGridAggregation) SubAggregationFields() []string {
	if gga.subAggBuilders == nil {
		return nil
	}
	fieldSet := make(map[string]bool)
	for _, subAgg := range gga.subAggBuilders {
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

func (gga *GeohashGridAggregation) StartDoc() {
	gga.sawValue = false
	gga.currentCell = ""
}

func (gga *GeohashGridAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, extract geo point and compute geohash
	if field == gga.field {
		if !gga.sawValue {
			gga.sawValue = true

			// Decode Morton hash to get lat/lon
			prefixCoded := numeric.PrefixCoded(term)
			shift, err := prefixCoded.Shift()
			if err == nil && shift == 0 {
				i64, err := prefixCoded.Int64()
				if err == nil {
					// Extract lon/lat from Morton hash
					lon := geo.MortonUnhashLon(uint64(i64))
					lat := geo.MortonUnhashLat(uint64(i64))

					// Encode to geohash and take prefix
					fullGeohash := geo.EncodeGeoHash(lat, lon)
					cellGeohash := fullGeohash[:gga.precision]
					gga.currentCell = cellGeohash

					// Increment count for this cell
					gga.cellCounts[cellGeohash]++

					// Initialize sub-aggregations for this cell if needed
					if gga.subAggBuilders != nil && len(gga.subAggBuilders) > 0 {
						if _, exists := gga.cellSubAggs[cellGeohash]; !exists {
							gga.cellSubAggs[cellGeohash] = &subAggregationSet{
								builders: gga.cloneSubAggBuilders(),
							}
						}
						// Start document processing for this cell's sub-aggregations
						if subAggs, exists := gga.cellSubAggs[cellGeohash]; exists {
							for _, subAgg := range subAggs.builders {
								subAgg.StartDoc()
							}
						}
					}
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current cell
	if gga.currentCell != "" && gga.subAggBuilders != nil {
		if subAggs, exists := gga.cellSubAggs[gga.currentCell]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.UpdateVisitor(field, term)
			}
		}
	}
}

func (gga *GeohashGridAggregation) EndDoc() {
	if gga.sawValue && gga.currentCell != "" && gga.subAggBuilders != nil {
		// End document for all sub-aggregations in this cell
		if subAggs, exists := gga.cellSubAggs[gga.currentCell]; exists {
			for _, subAgg := range subAggs.builders {
				subAgg.EndDoc()
			}
		}
	}
}

func (gga *GeohashGridAggregation) Result() *search.AggregationResult {
	// Sort cells by count (descending) and take top N
	type cellCount struct {
		geohash string
		count   int64
	}

	cells := make([]cellCount, 0, len(gga.cellCounts))
	for cell, count := range gga.cellCounts {
		cells = append(cells, cellCount{cell, count})
	}

	sort.Slice(cells, func(i, j int) bool {
		return cells[i].count > cells[j].count
	})

	// Limit to size
	if len(cells) > gga.size {
		cells = cells[:gga.size]
	}

	// Build buckets with sub-aggregation results
	buckets := make([]*search.Bucket, len(cells))
	for i, cc := range cells {
		// Decode geohash to get representative lat/lon (center of cell)
		lat, lon := geo.DecodeGeoHash(cc.geohash)

		bucket := &search.Bucket{
			Key:   cc.geohash,
			Count: cc.count,
			// Store the center point of the geohash cell
			Metadata: map[string]interface{}{
				"lat": lat,
				"lon": lon,
			},
		}

		// Add sub-aggregation results for this bucket
		if subAggs, exists := gga.cellSubAggs[cc.geohash]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		buckets[i] = bucket
	}

	return &search.AggregationResult{
		Field:   gga.field,
		Type:    "geohash_grid",
		Buckets: buckets,
	}
}

func (gga *GeohashGridAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if gga.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(gga.subAggBuilders))
		for name, subAgg := range gga.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	return NewGeohashGridAggregation(gga.field, gga.precision, gga.size, clonedSubAggs)
}

func (gga *GeohashGridAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(gga.subAggBuilders))
	for name, builder := range gga.subAggBuilders {
		cloned[name] = builder.Clone()
	}
	return cloned
}

// GeoDistanceAggregation groups geo points by distance ranges from a center point
type GeoDistanceAggregation struct {
	field          string
	centerLon      float64
	centerLat      float64
	unit           float64 // multiplier to convert to meters
	ranges         map[string]*DistanceRange
	rangeCounts    map[string]int64
	rangeSubAggs   map[string]*subAggregationSet
	subAggBuilders map[string]search.AggregationBuilder
	currentRanges  []string // ranges the current point falls into
	sawValue       bool
}

// DistanceRange represents a distance range for geo distance aggregations
type DistanceRange struct {
	Name string
	From *float64 // in specified units
	To   *float64 // in specified units
}

// NewGeoDistanceAggregation creates a new geo distance aggregation
// centerLon, centerLat: center point for distance calculation
// unit: distance unit multiplier (e.g., 1000 for kilometers, 1 for meters)
func NewGeoDistanceAggregation(field string, centerLon, centerLat float64, unit float64, ranges map[string]*DistanceRange, subAggregations map[string]search.AggregationBuilder) *GeoDistanceAggregation {
	if unit <= 0 {
		unit = 1000 // default to kilometers
	}

	return &GeoDistanceAggregation{
		field:          field,
		centerLon:      centerLon,
		centerLat:      centerLat,
		unit:           unit,
		ranges:         ranges,
		rangeCounts:    make(map[string]int64),
		rangeSubAggs:   make(map[string]*subAggregationSet),
		subAggBuilders: subAggregations,
		currentRanges:  make([]string, 0, len(ranges)),
	}
}

func (gda *GeoDistanceAggregation) Size() int {
	return reflectStaticSizeGeoDistanceAggregation + size.SizeOfPtr + len(gda.field)
}

func (gda *GeoDistanceAggregation) Field() string {
	return gda.field
}

func (gda *GeoDistanceAggregation) Type() string {
	return "geo_distance"
}

func (gda *GeoDistanceAggregation) SubAggregationFields() []string {
	if gda.subAggBuilders == nil {
		return nil
	}
	fieldSet := make(map[string]bool)
	for _, subAgg := range gda.subAggBuilders {
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

func (gda *GeoDistanceAggregation) StartDoc() {
	gda.sawValue = false
	gda.currentRanges = gda.currentRanges[:0]
}

func (gda *GeoDistanceAggregation) UpdateVisitor(field string, term []byte) {
	// If this is our field, compute distance and determine which ranges it falls into
	if field == gda.field {
		if !gda.sawValue {
			gda.sawValue = true

			// Decode Morton hash to get lat/lon
			prefixCoded := numeric.PrefixCoded(term)
			shift, err := prefixCoded.Shift()
			if err == nil && shift == 0 {
				i64, err := prefixCoded.Int64()
				if err == nil {
					// Extract lon/lat from Morton hash
					lon := geo.MortonUnhashLon(uint64(i64))
					lat := geo.MortonUnhashLat(uint64(i64))

					// Calculate distance using Haversin formula (returns kilometers)
					distanceKm := geo.Haversin(gda.centerLon, gda.centerLat, lon, lat)
					// Convert to meters then to specified unit
					distanceInUnit := (distanceKm * 1000) / gda.unit

					// Check which ranges this distance falls into
					for rangeName, r := range gda.ranges {
						inRange := true
						if r.From != nil && distanceInUnit < *r.From {
							inRange = false
						}
						if r.To != nil && distanceInUnit >= *r.To {
							inRange = false
						}
						if inRange {
							gda.rangeCounts[rangeName]++
							gda.currentRanges = append(gda.currentRanges, rangeName)

							// Initialize sub-aggregations for this range if needed
							if gda.subAggBuilders != nil && len(gda.subAggBuilders) > 0 {
								if _, exists := gda.rangeSubAggs[rangeName]; !exists {
									gda.rangeSubAggs[rangeName] = &subAggregationSet{
										builders: gda.cloneSubAggBuilders(),
									}
								}
							}
						}
					}

					// Start document processing for sub-aggregations in all ranges this document falls into
					if gda.subAggBuilders != nil && len(gda.subAggBuilders) > 0 {
						for _, rangeName := range gda.currentRanges {
							if subAggs, exists := gda.rangeSubAggs[rangeName]; exists {
								for _, subAgg := range subAggs.builders {
									subAgg.StartDoc()
								}
							}
						}
					}
				}
			}
		}
	}

	// Forward all field values to sub-aggregations in the current ranges
	if gda.subAggBuilders != nil {
		for _, rangeName := range gda.currentRanges {
			if subAggs, exists := gda.rangeSubAggs[rangeName]; exists {
				for _, subAgg := range subAggs.builders {
					subAgg.UpdateVisitor(field, term)
				}
			}
		}
	}
}

func (gda *GeoDistanceAggregation) EndDoc() {
	if gda.sawValue && gda.subAggBuilders != nil {
		// End document for all affected ranges
		for _, rangeName := range gda.currentRanges {
			if subAggs, exists := gda.rangeSubAggs[rangeName]; exists {
				for _, subAgg := range subAggs.builders {
					subAgg.EndDoc()
				}
			}
		}
	}
}

func (gda *GeoDistanceAggregation) Result() *search.AggregationResult {
	buckets := make([]*search.Bucket, 0, len(gda.ranges))

	for rangeName, r := range gda.ranges {
		bucket := &search.Bucket{
			Key:   rangeName,
			Count: gda.rangeCounts[rangeName],
			Metadata: map[string]interface{}{
				"from": r.From,
				"to":   r.To,
			},
		}

		// Add sub-aggregation results
		if subAggs, exists := gda.rangeSubAggs[rangeName]; exists {
			bucket.Aggregations = make(map[string]*search.AggregationResult)
			for name, subAgg := range subAggs.builders {
				bucket.Aggregations[name] = subAgg.Result()
			}
		}

		buckets = append(buckets, bucket)
	}

	// Sort buckets by from distance (ascending)
	sort.Slice(buckets, func(i, j int) bool {
		fromI := buckets[i].Metadata["from"]
		fromJ := buckets[j].Metadata["from"]
		if fromI == nil {
			return true
		}
		if fromJ == nil {
			return false
		}
		return *fromI.(*float64) < *fromJ.(*float64)
	})

	return &search.AggregationResult{
		Field:   gda.field,
		Type:    "geo_distance",
		Buckets: buckets,
		Metadata: map[string]interface{}{
			"center_lat": gda.centerLat,
			"center_lon": gda.centerLon,
		},
	}
}

func (gda *GeoDistanceAggregation) Clone() search.AggregationBuilder {
	// Clone sub-aggregations
	var clonedSubAggs map[string]search.AggregationBuilder
	if gda.subAggBuilders != nil {
		clonedSubAggs = make(map[string]search.AggregationBuilder, len(gda.subAggBuilders))
		for name, subAgg := range gda.subAggBuilders {
			clonedSubAggs[name] = subAgg.Clone()
		}
	}

	// Deep copy ranges
	clonedRanges := make(map[string]*DistanceRange, len(gda.ranges))
	for name, r := range gda.ranges {
		clonedRange := &DistanceRange{
			Name: r.Name,
		}
		if r.From != nil {
			from := *r.From
			clonedRange.From = &from
		}
		if r.To != nil {
			to := *r.To
			clonedRange.To = &to
		}
		clonedRanges[name] = clonedRange
	}

	return NewGeoDistanceAggregation(gda.field, gda.centerLon, gda.centerLat, gda.unit, clonedRanges, clonedSubAggs)
}

func (gda *GeoDistanceAggregation) cloneSubAggBuilders() map[string]search.AggregationBuilder {
	cloned := make(map[string]search.AggregationBuilder, len(gda.subAggBuilders))
	for name, builder := range gda.subAggBuilders {
		cloned[name] = builder.Clone()
	}
	return cloned
}

// Helper function to calculate midpoint of a geohash cell (for bucket metadata)
func geohashCellCenter(geohash string) (lat, lon float64) {
	return geo.DecodeGeoHash(geohash)
}

// Helper to check if distance is within range bounds
func inDistanceRange(distance float64, from, to *float64) bool {
	if from != nil && distance < *from {
		return false
	}
	if to != nil && distance >= *to {
		return false
	}
	return true
}

// Helper to convert distance to specified unit
func convertDistance(distanceMeters float64, unit string) float64 {
	unitMultiplier, _ := geo.ParseDistanceUnit(unit)
	if unitMultiplier > 0 {
		return distanceMeters / unitMultiplier
	}
	return distanceMeters
}

// Helper to parse unit string and return multiplier for converting FROM meters
func parseDistanceUnitMultiplier(unit string) float64 {
	if unit == "" {
		return 1000 // default to kilometers
	}
	multiplier, err := geo.ParseDistanceUnit(unit)
	if err != nil {
		return 1000 // fallback to kilometers
	}
	return multiplier
}

// IsNaN checks if a float64 is NaN
func isNaN(f float64) bool {
	return math.IsNaN(f)
}
