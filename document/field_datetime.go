//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package document

import (
	"fmt"
	"math"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/numeric_util"
)

const DEFAULT_DATETIME_INDEXING_OPTIONS = STORE_FIELD | INDEX_FIELD
const DEFAULT_DATETIME_PRECISION_STEP uint = 4

var MinTimeRepresentable = time.Unix(0, math.MinInt64)
var MaxTimeRepresentable = time.Unix(0, math.MaxInt64)

type DateTimeField struct {
	name           string
	arrayPositions []uint64
	options        IndexingOptions
	value          numeric_util.PrefixCoded
}

func (n *DateTimeField) Name() string {
	return n.name
}

func (n *DateTimeField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *DateTimeField) Options() IndexingOptions {
	return n.options
}

func (n *DateTimeField) Analyze() (int, analysis.TokenFrequencies) {
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

		shift := DEFAULT_PRECISION_STEP
		for shift < 64 {
			shiftEncoded, err := numeric_util.NewPrefixCodedInt64(original, shift)
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
			shift += DEFAULT_PRECISION_STEP
		}
	}

	fieldLength := len(tokens)
	tokenFreqs := analysis.TokenFrequency(tokens)
	return fieldLength, tokenFreqs
}

func (n *DateTimeField) Value() []byte {
	return n.value
}

func (n *DateTimeField) DateTime() (time.Time, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, i64), nil
}

func (n *DateTimeField) GoString() string {
	return fmt.Sprintf("&document.DateField{Name:%s, Options: %s, Value: %s}", n.name, n.options, n.value)
}

func NewDateTimeFieldFromBytes(name string, arrayPositions []uint64, value []byte) *DateTimeField {
	return &DateTimeField{
		name:           name,
		arrayPositions: arrayPositions,
		value:          value,
		options:        DEFAULT_DATETIME_INDEXING_OPTIONS,
	}
}

func NewDateTimeField(name string, arrayPositions []uint64, dt time.Time) (*DateTimeField, error) {
	return NewDateTimeFieldWithIndexingOptions(name, arrayPositions, dt, DEFAULT_DATETIME_INDEXING_OPTIONS)
}

func NewDateTimeFieldWithIndexingOptions(name string, arrayPositions []uint64, dt time.Time, options IndexingOptions) (*DateTimeField, error) {
	if canRepresent(dt) {
		dtInt64 := dt.UnixNano()
		prefixCoded := numeric_util.MustNewPrefixCodedInt64(dtInt64, 0)
		return &DateTimeField{
			name:           name,
			arrayPositions: arrayPositions,
			value:          prefixCoded,
			options:        options,
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
