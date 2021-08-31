//  Copyright (c) 2021 Couchbase, Inc.
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
	"encoding/binary"
	"math"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

type NumericRangeField struct {
	name           string
	arrayPositions []uint64
	options        index.FieldIndexingOptions
	value          []byte
}

func (n *NumericRangeField) Size() int {
	return reflectStaticSizeNumericField + size.SizeOfPtr +
		len(n.name) +
		len(n.arrayPositions)*size.SizeOfPtr
}

func (n *NumericRangeField) Name() string {
	return n.name
}

func (n *NumericRangeField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *NumericRangeField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *NumericRangeField) EncodedFieldType() byte {
	return 'r'
}

func (n *NumericRangeField) AnalyzedLength() int {
	return 0
}

func (n *NumericRangeField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

func (n *NumericRangeField) Analyze() {

}

func (n *NumericRangeField) Value() []byte {
	return n.value
}

func (n *NumericRangeField) NumPlainTextBytes() uint64 {
	return 0
}

func (n *NumericRangeField) Number() (float64, error) {
	return math.Float64frombits(binary.BigEndian.Uint64(n.value)), nil
}

func NewNumericRangeField(name string, arrayPositions []uint64, number float64) *NumericRangeField {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, math.Float64bits(number))
	return &NumericRangeField{
		name:           name,
		arrayPositions: arrayPositions,
		value:          value,
	}
}
