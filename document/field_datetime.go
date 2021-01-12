//  Copyright (c) 2014 Couchbase, Inc.
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
	"math"
	"reflect"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeDateTimeField int

func init() {
	var f DateTimeField
	reflectStaticSizeDateTimeField = int(reflect.TypeOf(f).Size())
}

const DefaultDateTimeIndexingOptions = index.StoreField | index.IndexField | index.DocValues
const DefaultDateTimePrecisionStep uint = 4

var MinTimeRepresentable = time.Unix(0, math.MinInt64)
var MaxTimeRepresentable = time.Unix(0, math.MaxInt64)

type DateTimeField struct {
	name              string
	arrayPositions    []uint64
	options           index.FieldIndexingOptions
	value             numeric.PrefixCoded
	numPlainTextBytes uint64
	length            int
	frequencies       index.TokenFrequencies
}

func (n *DateTimeField) Size() int {
	return reflectStaticSizeDateTimeField + size.SizeOfPtr +
		len(n.name) +
		len(n.arrayPositions)*size.SizeOfUint64
}

func (n *DateTimeField) Name() string {
	return n.name
}

func (n *DateTimeField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *DateTimeField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *DateTimeField) EncodedFieldType() byte {
	return 'd'
}

func (n *DateTimeField) AnalyzedLength() int {
	return n.length
}

func (n *DateTimeField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return n.frequencies
}

func (n *DateTimeField) Analyze() {
	tokens := make(analysis.TokenStream, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.DateTime,
	})

	original, err := n.value.Int64()
	if err == nil {

		shift := DefaultDateTimePrecisionStep
		for shift < 64 {
			shiftEncoded, err := numeric.NewPrefixCodedInt64(original, shift)
			if err != nil {
				break
			}
			token := analysis.Token{
				Start:    0,
				End:      len(shiftEncoded),
				Term:     shiftEncoded,
				Position: 1,
				Type:     analysis.DateTime,
			}
			tokens = append(tokens, &token)
			shift += DefaultDateTimePrecisionStep
		}
	}

	n.length = len(tokens)
	n.frequencies = analysis.TokenFrequency(tokens, n.arrayPositions, n.options)
}

func (n *DateTimeField) Value() []byte {
	return n.value
}

func (n *DateTimeField) DateTime() (time.Time, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, i64).UTC(), nil
}

func (n *DateTimeField) GoString() string {
	return fmt.Sprintf("&document.DateField{Name:%s, Options: %s, Value: %s}", n.name, n.options, n.value)
}

func (n *DateTimeField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewDateTimeFieldFromBytes(name string, arrayPositions []uint64, value []byte) *DateTimeField {
	return &DateTimeField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		options:           DefaultDateTimeIndexingOptions,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewDateTimeField(name string, arrayPositions []uint64, dt time.Time) (*DateTimeField, error) {
	return NewDateTimeFieldWithIndexingOptions(name, arrayPositions, dt, DefaultDateTimeIndexingOptions)
}

func NewDateTimeFieldWithIndexingOptions(name string, arrayPositions []uint64, dt time.Time, options index.FieldIndexingOptions) (*DateTimeField, error) {
	if canRepresent(dt) {
		dtInt64 := dt.UnixNano()
		prefixCoded := numeric.MustNewPrefixCodedInt64(dtInt64, 0)
		return &DateTimeField{
			name:           name,
			arrayPositions: arrayPositions,
			value:          prefixCoded,
			options:        options,
			// not correct, just a place holder until we revisit how fields are
			// represented and can fix this better
			numPlainTextBytes: uint64(8),
		}, nil
	}
	return nil, fmt.Errorf("cannot represent %s in this type", dt)
}

func canRepresent(dt time.Time) bool {
	if dt.Before(MinTimeRepresentable) || dt.After(MaxTimeRepresentable) {
		return false
	}
	return true
}
