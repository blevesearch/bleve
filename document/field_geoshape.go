//  Copyright (c) 2022 Couchbase, Inc.
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

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeGeoShapeField int

func init() {
	var f GeoShapeField
	reflectStaticSizeGeoShapeField = int(reflect.TypeOf(f).Size())
}

const DefaultGeoShapeIndexingOptions = index.IndexField | index.DocValues

type GeoShapeField struct {
	name              string
	shape             index.GeoJSON
	arrayPositions    []uint64
	options           index.FieldIndexingOptions
	numPlainTextBytes uint64
	length            int
	value             []byte

	frequencies index.TokenFrequencies
}

func (n *GeoShapeField) Size() int {
	return reflectStaticSizeGeoShapeField + size.SizeOfPtr +
		len(n.name) +
		len(n.arrayPositions)*size.SizeOfUint64
}

func (n *GeoShapeField) Name() string {
	return n.name
}

func (n *GeoShapeField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *GeoShapeField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *GeoShapeField) EncodedFieldType() byte {
	return 's'
}

func (n *GeoShapeField) AnalyzedLength() int {
	return n.length
}

func (n *GeoShapeField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return n.frequencies
}

func (n *GeoShapeField) Analyze() {
	// compute the bytes representation for the coordinates
	tokens := make(analysis.TokenStream, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.AlphaNumeric,
	})

	rti := geo.GetSpatialAnalyzerPlugin("s2")
	terms := rti.GetIndexTokens(n.shape)

	for _, term := range terms {
		token := analysis.Token{
			Start:    0,
			End:      len(term),
			Term:     []byte(term),
			Position: 1,
			Type:     analysis.AlphaNumeric,
		}
		tokens = append(tokens, &token)
	}

	n.length = len(tokens)
	n.frequencies = analysis.TokenFrequency(tokens, n.arrayPositions, n.options)
}

func (n *GeoShapeField) Value() []byte {
	return n.value
}

func (n *GeoShapeField) GoString() string {
	return fmt.Sprintf("&document.GeoShapeField{Name:%s, Options: %s, Value: %s}", n.name, n.options, n.value)
}

func (n *GeoShapeField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func NewGeoShapeField(name string, arrayPositions []uint64,
	coordinates [][][][]float64, typ string) *GeoShapeField {
	return NewGeoShapeFieldWithIndexingOptions(name, arrayPositions,
		coordinates, typ, DefaultGeoShapeIndexingOptions)
}

func NewGeoShapeFieldWithIndexingOptions(name string, arrayPositions []uint64,
	coordinates [][][][]float64, typ string,
	options index.FieldIndexingOptions) *GeoShapeField {
	shape, value, err := geo.NewGeoJsonShape(coordinates, typ)
	if err != nil {
		return nil
	}

	// extra glue bytes to work around the term splitting logic from interfering
	// the custom encoding of the geoshape coordinates inside the docvalues.
	value = append(geo.GlueBytes, append(value, geo.GlueBytes...)...)

	options = options | DefaultGeoShapeIndexingOptions

	return &GeoShapeField{
		shape:             shape,
		name:              name,
		arrayPositions:    arrayPositions,
		options:           options,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewGeometryCollectionFieldWithIndexingOptions(name string,
	arrayPositions []uint64, coordinates [][][][][]float64, types []string,
	options index.FieldIndexingOptions) *GeoShapeField {
	shape, value, err := geo.NewGeometryCollection(coordinates, types)
	if err != nil {
		return nil
	}

	// extra glue bytes to work around the term splitting logic from interfering
	// the custom encoding of the geoshape coordinates inside the docvalues.
	value = append(geo.GlueBytes, append(value, geo.GlueBytes...)...)

	options = options | DefaultGeoShapeIndexingOptions

	return &GeoShapeField{
		shape:             shape,
		name:              name,
		arrayPositions:    arrayPositions,
		options:           options,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewGeoCircleFieldWithIndexingOptions(name string, arrayPositions []uint64,
	centerPoint []float64, radius float64,
	options index.FieldIndexingOptions) *GeoShapeField {
	shape, value, err := geo.NewGeoCircleShape(centerPoint, radius)
	if err != nil {
		return nil
	}

	// extra glue bytes to work around the term splitting logic from interfering
	// the custom encoding of the geoshape coordinates inside the docvalues.
	value = append(geo.GlueBytes, append(value, geo.GlueBytes...)...)

	options = options | DefaultGeoShapeIndexingOptions

	return &GeoShapeField{
		shape:             shape,
		name:              name,
		arrayPositions:    arrayPositions,
		options:           options,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}
