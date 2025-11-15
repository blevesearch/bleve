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
	"reflect"

	"github.com/axiomhq/hyperloglog"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizeCardinalityAggregation int

func init() {
	var ca CardinalityAggregation
	reflectStaticSizeCardinalityAggregation = int(reflect.TypeOf(ca).Size())
}

// CardinalityAggregation computes approximate unique value count using HyperLogLog++
type CardinalityAggregation struct {
	field     string
	hll       *hyperloglog.Sketch
	precision uint8
	sawValue  bool
}

// NewCardinalityAggregation creates a new cardinality aggregation
// precision controls accuracy vs memory tradeoff:
//   - 10: 1KB, ~2.6% error
//   - 12: 4KB, ~1.6% error
//   - 14: 16KB, ~0.81% error (default)
//   - 16: 64KB, ~0.41% error
func NewCardinalityAggregation(field string, precision uint8) *CardinalityAggregation {
	if precision == 0 {
		precision = 14 // Default: good balance of accuracy and memory
	}

	// Create HyperLogLog sketch with specified precision
	hll, err := hyperloglog.NewSketch(precision, true) // sparse=true for better memory efficiency
	if err != nil {
		// Fallback to default precision 14 if invalid precision specified
		hll, _ = hyperloglog.NewSketch(14, true)
		precision = 14
	}

	return &CardinalityAggregation{
		field:     field,
		hll:       hll,
		precision: precision,
	}
}

func (ca *CardinalityAggregation) Size() int {
	sizeInBytes := reflectStaticSizeCardinalityAggregation + size.SizeOfPtr + len(ca.field)

	// HyperLogLog sketch size: 2^precision bytes
	sizeInBytes += 1 << ca.precision

	return sizeInBytes
}

func (ca *CardinalityAggregation) Field() string {
	return ca.field
}

func (ca *CardinalityAggregation) Type() string {
	return "cardinality"
}

func (ca *CardinalityAggregation) StartDoc() {
	ca.sawValue = false
}

func (ca *CardinalityAggregation) UpdateVisitor(field string, term []byte) {
	if field != ca.field {
		return
	}
	ca.sawValue = true

	// Insert term into HyperLogLog sketch
	// HyperLogLog handles hashing internally
	ca.hll.Insert(term)
}

func (ca *CardinalityAggregation) EndDoc() {
	// Nothing to do
}

func (ca *CardinalityAggregation) Result() *search.AggregationResult {
	cardinality := int64(ca.hll.Estimate())

	// Serialize sketch for distributed merging
	sketchBytes, err := ca.hll.MarshalBinary()
	if err != nil {
		sketchBytes = nil
	}

	return &search.AggregationResult{
		Field: ca.field,
		Type:  "cardinality",
		Value: &search.CardinalityResult{
			Cardinality: cardinality,
			Sketch:      sketchBytes,
			HLL:         ca.hll, // Keep in-memory reference for local merging
		},
	}
}

func (ca *CardinalityAggregation) Clone() search.AggregationBuilder {
	return NewCardinalityAggregation(ca.field, ca.precision)
}
