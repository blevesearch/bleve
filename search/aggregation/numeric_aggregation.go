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

	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var (
	reflectStaticSizeSumAggregation       int
	reflectStaticSizeAvgAggregation       int
	reflectStaticSizeMinAggregation       int
	reflectStaticSizeMaxAggregation       int
	reflectStaticSizeCountAggregation     int
	reflectStaticSizeSumSquaresAggregation int
	reflectStaticSizeStatsAggregation     int
)

func init() {
	var sa SumAggregation
	reflectStaticSizeSumAggregation = int(reflect.TypeOf(sa).Size())
	var aa AvgAggregation
	reflectStaticSizeAvgAggregation = int(reflect.TypeOf(aa).Size())
	var mina MinAggregation
	reflectStaticSizeMinAggregation = int(reflect.TypeOf(mina).Size())
	var maxa MaxAggregation
	reflectStaticSizeMaxAggregation = int(reflect.TypeOf(maxa).Size())
	var ca CountAggregation
	reflectStaticSizeCountAggregation = int(reflect.TypeOf(ca).Size())
	var ssa SumSquaresAggregation
	reflectStaticSizeSumSquaresAggregation = int(reflect.TypeOf(ssa).Size())
	var sta StatsAggregation
	reflectStaticSizeStatsAggregation = int(reflect.TypeOf(sta).Size())
}

// SumAggregation computes the sum of numeric values
type SumAggregation struct {
	field    string
	sum      float64
	count    int64
	sawValue bool
}

// NewSumAggregation creates a new sum aggregation
func NewSumAggregation(field string) *SumAggregation {
	return &SumAggregation{
		field: field,
	}
}

func (sa *SumAggregation) Size() int {
	return reflectStaticSizeSumAggregation + size.SizeOfPtr + len(sa.field)
}

func (sa *SumAggregation) Field() string {
	return sa.field
}

func (sa *SumAggregation) Type() string {
	return "sum"
}

func (sa *SumAggregation) StartDoc() {
	sa.sawValue = false
}

func (sa *SumAggregation) UpdateVisitor(field string, term []byte) {
	// Only process values for our field
	if field != sa.field {
		return
	}
	sa.sawValue = true
	// only consider values with shift 0 (full precision)
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			sa.sum += f64
			sa.count++
		}
	}
}

func (sa *SumAggregation) EndDoc() {
	// Nothing to do
}

func (sa *SumAggregation) Result() *search.AggregationResult {
	return &search.AggregationResult{
		Field: sa.field,
		Type:  "sum",
		Value: sa.sum,
	}
}

// AvgAggregation computes the average of numeric values
type AvgAggregation struct {
	field    string
	sum      float64
	count    int64
	sawValue bool
}

// NewAvgAggregation creates a new average aggregation
func NewAvgAggregation(field string) *AvgAggregation {
	return &AvgAggregation{
		field: field,
	}
}

func (aa *AvgAggregation) Size() int {
	return reflectStaticSizeAvgAggregation + size.SizeOfPtr + len(aa.field)
}

func (aa *AvgAggregation) Field() string {
	return aa.field
}

func (aa *AvgAggregation) Type() string {
	return "avg"
}

func (aa *AvgAggregation) StartDoc() {
	aa.sawValue = false
}

func (aa *AvgAggregation) UpdateVisitor(field string, term []byte) {
	if field != aa.field {
		return
	}
	aa.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			aa.sum += f64
			aa.count++
		}
	}
}

func (aa *AvgAggregation) EndDoc() {
	// Nothing to do
}

func (aa *AvgAggregation) Result() *search.AggregationResult {
	var avg float64
	if aa.count > 0 {
		avg = aa.sum / float64(aa.count)
	}
	return &search.AggregationResult{
		Field: aa.field,
		Type:  "avg",
		Value: avg,
	}
}

// MinAggregation computes the minimum value
type MinAggregation struct {
	field    string
	min      float64
	sawValue bool
}

// NewMinAggregation creates a new minimum aggregation
func NewMinAggregation(field string) *MinAggregation {
	return &MinAggregation{
		field: field,
		min:   math.MaxFloat64,
	}
}

func (ma *MinAggregation) Size() int {
	return reflectStaticSizeMinAggregation + size.SizeOfPtr + len(ma.field)
}

func (ma *MinAggregation) Field() string {
	return ma.field
}

func (ma *MinAggregation) Type() string {
	return "min"
}

func (ma *MinAggregation) StartDoc() {
	ma.sawValue = false
}

func (ma *MinAggregation) UpdateVisitor(field string, term []byte) {
	if field != ma.field {
		return
	}
	ma.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			if f64 < ma.min {
				ma.min = f64
			}
		}
	}
}

func (ma *MinAggregation) EndDoc() {
	// Nothing to do
}

func (ma *MinAggregation) Result() *search.AggregationResult {
	value := ma.min
	if !ma.sawValue {
		value = 0
	}
	return &search.AggregationResult{
		Field: ma.field,
		Type:  "min",
		Value: value,
	}
}

// MaxAggregation computes the maximum value
type MaxAggregation struct {
	field    string
	max      float64
	sawValue bool
}

// NewMaxAggregation creates a new maximum aggregation
func NewMaxAggregation(field string) *MaxAggregation {
	return &MaxAggregation{
		field: field,
		max:   -math.MaxFloat64,
	}
}

func (ma *MaxAggregation) Size() int {
	return reflectStaticSizeMaxAggregation + size.SizeOfPtr + len(ma.field)
}

func (ma *MaxAggregation) Field() string {
	return ma.field
}

func (ma *MaxAggregation) Type() string {
	return "max"
}

func (ma *MaxAggregation) StartDoc() {
	ma.sawValue = false
}

func (ma *MaxAggregation) UpdateVisitor(field string, term []byte) {
	if field != ma.field {
		return
	}
	ma.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			if f64 > ma.max {
				ma.max = f64
			}
		}
	}
}

func (ma *MaxAggregation) EndDoc() {
	// Nothing to do
}

func (ma *MaxAggregation) Result() *search.AggregationResult {
	value := ma.max
	if !ma.sawValue {
		value = 0
	}
	return &search.AggregationResult{
		Field: ma.field,
		Type:  "max",
		Value: value,
	}
}

// CountAggregation counts the number of values
type CountAggregation struct {
	field    string
	count    int64
	sawValue bool
}

// NewCountAggregation creates a new count aggregation
func NewCountAggregation(field string) *CountAggregation {
	return &CountAggregation{
		field: field,
	}
}

func (ca *CountAggregation) Size() int {
	return reflectStaticSizeCountAggregation + size.SizeOfPtr + len(ca.field)
}

func (ca *CountAggregation) Field() string {
	return ca.field
}

func (ca *CountAggregation) Type() string {
	return "count"
}

func (ca *CountAggregation) StartDoc() {
	ca.sawValue = false
}

func (ca *CountAggregation) UpdateVisitor(field string, term []byte) {
	if field != ca.field {
		return
	}
	ca.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		ca.count++
	}
}

func (ca *CountAggregation) EndDoc() {
	// Nothing to do
}

func (ca *CountAggregation) Result() *search.AggregationResult {
	return &search.AggregationResult{
		Field: ca.field,
		Type:  "count",
		Value: ca.count,
	}
}

// SumSquaresAggregation computes the sum of squares
type SumSquaresAggregation struct {
	field      string
	sumSquares float64
	count      int64
	sawValue   bool
}

// NewSumSquaresAggregation creates a new sum of squares aggregation
func NewSumSquaresAggregation(field string) *SumSquaresAggregation {
	return &SumSquaresAggregation{
		field: field,
	}
}

func (ssa *SumSquaresAggregation) Size() int {
	return reflectStaticSizeSumSquaresAggregation + size.SizeOfPtr + len(ssa.field)
}

func (ssa *SumSquaresAggregation) Field() string {
	return ssa.field
}

func (ssa *SumSquaresAggregation) Type() string {
	return "sumsquares"
}

func (ssa *SumSquaresAggregation) StartDoc() {
	ssa.sawValue = false
}

func (ssa *SumSquaresAggregation) UpdateVisitor(field string, term []byte) {
	if field != ssa.field {
		return
	}
	ssa.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			ssa.sumSquares += f64 * f64
			ssa.count++
		}
	}
}

func (ssa *SumSquaresAggregation) EndDoc() {
	// Nothing to do
}

func (ssa *SumSquaresAggregation) Result() *search.AggregationResult {
	return &search.AggregationResult{
		Field: ssa.field,
		Type:  "sumsquares",
		Value: ssa.sumSquares,
	}
}

// StatsAggregation computes comprehensive statistics including standard deviation
type StatsAggregation struct {
	field      string
	sum        float64
	sumSquares float64
	count      int64
	min        float64
	max        float64
	sawValue   bool
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

// NewStatsAggregation creates a comprehensive stats aggregation
func NewStatsAggregation(field string) *StatsAggregation {
	return &StatsAggregation{
		field: field,
		min:   math.MaxFloat64,
		max:   -math.MaxFloat64,
	}
}

func (sta *StatsAggregation) Size() int {
	return reflectStaticSizeStatsAggregation + size.SizeOfPtr + len(sta.field)
}

func (sta *StatsAggregation) Field() string {
	return sta.field
}

func (sta *StatsAggregation) Type() string {
	return "stats"
}

func (sta *StatsAggregation) StartDoc() {
	sta.sawValue = false
}

func (sta *StatsAggregation) UpdateVisitor(field string, term []byte) {
	if field != sta.field {
		return
	}
	sta.sawValue = true
	prefixCoded := numeric.PrefixCoded(term)
	shift, err := prefixCoded.Shift()
	if err == nil && shift == 0 {
		i64, err := prefixCoded.Int64()
		if err == nil {
			f64 := numeric.Int64ToFloat64(i64)
			sta.sum += f64
			sta.sumSquares += f64 * f64
			sta.count++
			if f64 < sta.min {
				sta.min = f64
			}
			if f64 > sta.max {
				sta.max = f64
			}
		}
	}
}

func (sta *StatsAggregation) EndDoc() {
	// Nothing to do
}

func (sta *StatsAggregation) Result() *search.AggregationResult {
	result := &StatsResult{
		Count:      sta.count,
		Sum:        sta.sum,
		SumSquares: sta.sumSquares,
	}

	if sta.count > 0 {
		result.Avg = sta.sum / float64(sta.count)
		result.Min = sta.min
		result.Max = sta.max

		// Calculate variance and standard deviation
		// Variance = E[X^2] - E[X]^2
		avgSquares := sta.sumSquares / float64(sta.count)
		result.Variance = avgSquares - (result.Avg * result.Avg)

		// Ensure variance is non-negative (can be slightly negative due to floating point errors)
		if result.Variance < 0 {
			result.Variance = 0
		}
		result.StdDev = math.Sqrt(result.Variance)
	}

	return &search.AggregationResult{
		Field: sta.field,
		Type:  "stats",
		Value: result,
	}
}
