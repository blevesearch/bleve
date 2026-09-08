//  Copyright (c) 2026 Couchbase, Inc.
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

package document

import (
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/numericv2"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeNumericV2Field int

func init() {
	var f NumericV2Field
	reflectStaticSizeNumericV2Field = int(reflect.TypeOf(f).Size())
}

// DefaultNumericV2IndexingOptions mirrors the v1 numeric defaults, minus
// IncludeInAll: a number_v2 field produces no tokens, so it can never
// participate in the _all composite field.
const DefaultNumericV2IndexingOptions = index.StoreField | index.IndexField | index.DocValues

// NumericV2Field is a numeric field indexed into the number_v2 section rather
// than as prefix-coded terms in the inverted index. It carries the value in two
// encodings because its two consumers need different things: the section's
// sorted search array wants the sortable uint64, while the sort and facet paths
// visit doc values as prefix-coded terms.
type NumericV2Field struct {
	name              string
	arrayPositions    []uint64
	options           index.FieldIndexingOptions
	value             numeric.PrefixCoded
	numPlainTextBytes uint64
}

func (n *NumericV2Field) Size() int {
	return reflectStaticSizeNumericV2Field + size.SizeOfPtr +
		len(n.name) +
		len(n.arrayPositions)*size.SizeOfUint64 +
		len(n.value)
}

func (n *NumericV2Field) Name() string {
	return n.name
}

func (n *NumericV2Field) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *NumericV2Field) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *NumericV2Field) EncodedFieldType() byte {
	return 'm'
}

// Analyze is a no-op: this field type is not tokenized. The number_v2 section
// reads the value directly, and nothing about it needs analysis.
func (n *NumericV2Field) Analyze() {
}

func (n *NumericV2Field) AnalyzedLength() int {
	return 0
}

func (n *NumericV2Field) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

// Value returns the stored-field representation, which is the same
// prefix-coded encoding a v1 NumericField stores. Keeping the two identical is
// what lets the stored-field decode path reuse NewNumericFieldFromBytes.
func (n *NumericV2Field) Value() []byte {
	return n.value
}

// DocValueTerm returns the prefix-coded, zero-shift term written to this
// field's doc values. The sort and facet paths validate doc value bytes as
// prefix-coded and keep only shift-zero terms, so this encoding is required
// rather than incidental: handing them the raw sortable uint64 would make
// every document sort and facet as missing.
func (n *NumericV2Field) DocValueTerm() []byte {
	return n.value
}

// SortableValue returns the value encoded as a uint64 whose unsigned ordering
// matches the float64 ordering of the original number.
func (n *NumericV2Field) SortableValue() uint64 {
	i64, err := n.value.Int64()
	if err != nil {
		return 0
	}
	return numericv2.EncodeInt64(i64)
}

func (n *NumericV2Field) Number() (float64, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return 0.0, err
	}
	return numeric.Int64ToFloat64(i64), nil
}

func (n *NumericV2Field) GoString() string {
	return fmt.Sprintf("&document.NumericV2Field{Name:%s, Options: %s, Value: %s}",
		n.name, n.options, n.value)
}

func (n *NumericV2Field) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewNumericV2FieldFromBytes(name string, arrayPositions []uint64, value []byte) *NumericV2Field {
	return &NumericV2Field{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		options:           DefaultNumericV2IndexingOptions,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewNumericV2Field(name string, arrayPositions []uint64, number float64) *NumericV2Field {
	return NewNumericV2FieldWithIndexingOptions(name, arrayPositions, number,
		DefaultNumericV2IndexingOptions)
}

func NewNumericV2FieldWithIndexingOptions(name string, arrayPositions []uint64,
	number float64, options index.FieldIndexingOptions) *NumericV2Field {
	numberInt64 := numeric.Float64ToInt64(number)
	prefixCoded := numeric.MustNewPrefixCodedInt64(numberInt64, 0)
	return &NumericV2Field{
		name:           name,
		arrayPositions: arrayPositions,
		value:          prefixCoded,
		options:        options,
		// not correct, just a place holder until we revisit how fields are
		// represented and can fix this better
		numPlainTextBytes: uint64(8),
	}
}
