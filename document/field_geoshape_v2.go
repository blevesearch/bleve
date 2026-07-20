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
	"reflect"

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/geov2"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/geojson"
)

var reflectStaticSizeGeoShapeV2Field int

func init() {
	var f GeoShapeV2Field
	reflectStaticSizeGeoShapeV2Field = int(reflect.TypeOf(f).Size())
}

type GeoShapeV2Field struct {
	name string

	shape      index.GeoJSON
	inner      []uint64
	cross      []uint64
	scoreInner uint64
	scoreCross uint64
	bBoxBytes  []byte
	shapeBytes []byte

	options index.FieldIndexingOptions
}

func (f *GeoShapeV2Field) Name() string {
	return f.name
}

func (f *GeoShapeV2Field) ArrayPositions() []uint64 {
	return nil
}

func (f *GeoShapeV2Field) Options() index.FieldIndexingOptions {
	return f.options
}

func (f *GeoShapeV2Field) Analyze() {
	f.inner, f.cross = f.shape.IndexCells()
	f.scoreInner = geov2.CalcCellsScore(f.inner)
	f.scoreCross = geov2.CalcCellsScore(f.cross)

	if bBox, ok := f.shape.BoundingBox().(*geojson.Envelope); ok {
		bBoxBytes, err := bBox.Marshal()
		if err != nil {
			return
		}
		f.bBoxBytes = bBoxBytes
	}
}

func (f *GeoShapeV2Field) Value() []byte {
	return []byte{}
}

func (f *GeoShapeV2Field) NumPlainTextBytes() uint64 {
	return 0
}

func (f *GeoShapeV2Field) Size() int {
	return reflectStaticSizeGeoShapeV2Field + size.SizeOfPtr +
		len(f.name) +
		len(f.inner)*size.SizeOfUint64 +
		len(f.cross)*size.SizeOfUint64 +
		len(f.bBoxBytes) +
		len(f.shapeBytes)
}

func (f *GeoShapeV2Field) EncodedFieldType() byte {
	return 'o'
}

func (f *GeoShapeV2Field) AnalyzedLength() int {
	return 0
}

func (f *GeoShapeV2Field) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

func (f *GeoShapeV2Field) InnerCells() []uint64 {
	return f.inner
}

func (f *GeoShapeV2Field) CrossCells() []uint64 {
	return f.cross
}

func (f *GeoShapeV2Field) EncodedBoundingBox() []byte {
	return f.bBoxBytes
}

func (f *GeoShapeV2Field) EncodedShape() []byte {
	return f.shapeBytes
}

func (f *GeoShapeV2Field) Scores() (uint64, uint64) {
	return f.scoreInner, f.scoreCross
}

func NewGeoShapeV2FieldFromShapeWithIndexingOptions(name string, geoShape *geojson.GeoShape,
	options index.FieldIndexingOptions) *GeoShapeV2Field {

	var shape index.GeoJSON
	var shapeBytes []byte
	var err error

	if geoShape.Type == geo.CircleType {
		shape, shapeBytes, err = geo.NewGeoCircleShape(geoShape.Center,
			geoShape.Radius)
	} else {
		shape, shapeBytes, err = geo.NewGeoJsonShape(geoShape.Coordinates,
			geoShape.Type)
	}
	if err != nil {
		return nil
	}

	return &GeoShapeV2Field{
		name:       name,
		shape:      shape,
		options:    options,
		shapeBytes: shapeBytes,
	}
}

func NewGeometryCollectionV2FieldFromShapesWithIndexingOptions(name string,
	geoShapes []*geojson.GeoShape, options index.FieldIndexingOptions) *GeoShapeV2Field {
	shape, shapeBytes, err := geo.NewGeometryCollectionFromShapes(geoShapes)
	if err != nil {
		return nil
	}

	return &GeoShapeV2Field{
		name:       name,
		shape:      shape,
		options:    options,
		shapeBytes: shapeBytes,
	}
}
