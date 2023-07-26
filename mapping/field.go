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

package mapping

import (
	"net"
	"time"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	index "github.com/blevesearch/bleve_index_api"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
)

// control the default behavior for dynamic fields (those not explicitly mapped)
var (
	IndexDynamic     = true
	StoreDynamic     = true
	DocValuesDynamic = true // TODO revisit default?
)

// NewTextFieldMapping returns a default field mapping for text
func NewTextFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:               "text",
		Store:              true,
		Index:              true,
		IncludeTermVectors: true,
		IncludeInAll:       true,
		DocValues:          true,
	}
}

func newTextFieldMappingDynamic(im *IndexMappingImpl) *FieldMapping {
	rv := NewTextFieldMapping()
	rv.Store = im.StoreDynamic
	rv.Index = im.IndexDynamic
	rv.DocValues = im.DocValuesDynamic
	return rv
}

// NewKeyworFieldMapping returns a default field mapping for text with analyzer "keyword".
func NewKeywordFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:               "text",
		Analyzer:           keyword.Name,
		Store:              true,
		Index:              true,
		IncludeTermVectors: true,
		IncludeInAll:       true,
		DocValues:          true,
	}
}

// NewNumericFieldMapping returns a default field mapping for numbers
func NewNumericFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "number",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
		DocValues:    true,
	}
}

func newNumericFieldMappingDynamic(im *IndexMappingImpl) *FieldMapping {
	rv := NewNumericFieldMapping()
	rv.Store = im.StoreDynamic
	rv.Index = im.IndexDynamic
	rv.DocValues = im.DocValuesDynamic
	return rv
}

// NewDateTimeFieldMapping returns a default field mapping for dates
func NewDateTimeFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "datetime",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
		DocValues:    true,
	}
}

func newDateTimeFieldMappingDynamic(im *IndexMappingImpl) *FieldMapping {
	rv := NewDateTimeFieldMapping()
	rv.Store = im.StoreDynamic
	rv.Index = im.IndexDynamic
	rv.DocValues = im.DocValuesDynamic
	return rv
}

// NewBooleanFieldMapping returns a default field mapping for booleans
func NewBooleanFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "boolean",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
		DocValues:    true,
	}
}

func newBooleanFieldMappingDynamic(im *IndexMappingImpl) *FieldMapping {
	rv := NewBooleanFieldMapping()
	rv.Store = im.StoreDynamic
	rv.Index = im.IndexDynamic
	rv.DocValues = im.DocValuesDynamic
	return rv
}

// NewGeoPointFieldMapping returns a default field mapping for geo points
func NewGeoPointFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "geopoint",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
		DocValues:    true,
	}
}

// NewGeoShapeFieldMapping returns a default field mapping
// for geoshapes
func NewGeoShapeFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "geoshape",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
		DocValues:    true,
	}
}

// NewIPFieldMapping returns a default field mapping for IP points
func NewIPFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "IP",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
	}
}

// Options returns the indexing options for this field.
func (fm *FieldMapping) Options() index.FieldIndexingOptions {
	var rv index.FieldIndexingOptions
	if fm.Store {
		rv |= index.StoreField
	}
	if fm.Index {
		rv |= index.IndexField
	}
	if fm.IncludeTermVectors {
		rv |= index.IncludeTermVectors
	}
	if fm.DocValues {
		rv |= index.DocValues
	}
	if fm.SkipFreqNorm {
		rv |= index.SkipFreqNorm
	}
	return rv
}

func (fm *FieldMapping) processString(propertyValueString string, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	options := fm.Options()
	if fm.Type == "text" {
		analyzer := fm.analyzerForField(path, context)
		field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	} else if fm.Type == "datetime" {
		dateTimeFormat := context.im.DefaultDateTimeParser
		if fm.DateFormat != "" {
			dateTimeFormat = fm.DateFormat
		}
		dateTimeParser := context.im.DateTimeParserNamed(dateTimeFormat)
		if dateTimeParser != nil {
			parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
			if err == nil {
				fm.processTime(parsedDateTime, pathString, path, indexes, context)
			}
		}
	} else if fm.Type == "IP" {
		ip := net.ParseIP(propertyValueString)
		if ip != nil {
			fm.processIP(ip, pathString, path, indexes, context)
		}
	}
}

func (fm *FieldMapping) processFloat64(propertyValFloat float64, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "number" {
		options := fm.Options()
		field := document.NewNumericFieldWithIndexingOptions(fieldName, indexes, propertyValFloat, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processTime(propertyValueTime time.Time, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "datetime" {
		options := fm.Options()
		field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, propertyValueTime, options)
		if err == nil {
			context.doc.AddField(field)
		} else {
			logger.Printf("could not build date %v", err)
		}

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processBoolean(propertyValueBool bool, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "boolean" {
		options := fm.Options()
		field := document.NewBooleanFieldWithIndexingOptions(fieldName, indexes, propertyValueBool, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processGeoPoint(propertyMightBeGeoPoint interface{}, pathString string, path []string, indexes []uint64, context *walkContext) {
	lon, lat, found := geo.ExtractGeoPoint(propertyMightBeGeoPoint)
	if found {
		fieldName := getFieldName(pathString, path, fm)
		options := fm.Options()
		field := document.NewGeoPointFieldWithIndexingOptions(fieldName, indexes, lon, lat, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processIP(ip net.IP, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	options := fm.Options()
	field := document.NewIPFieldWithIndexingOptions(fieldName, indexes, ip, options)
	context.doc.AddField(field)

	if !fm.IncludeInAll {
		context.excludedFromAll = append(context.excludedFromAll, fieldName)
	}
}

func (fm *FieldMapping) processGeoShape(propertyMightBeGeoShape interface{},
	pathString string, path []string, indexes []uint64, context *walkContext) {
	coordValue, shape, err := geo.ParseGeoShapeField(propertyMightBeGeoShape)
	if err != nil {
		return
	}

	if shape == geo.CircleType {
		center, radius, found := geo.ExtractCircle(propertyMightBeGeoShape)
		if found {
			fieldName := getFieldName(pathString, path, fm)
			options := fm.Options()
			field := document.NewGeoCircleFieldWithIndexingOptions(fieldName,
				indexes, center, radius, options)
			context.doc.AddField(field)

			if !fm.IncludeInAll {
				context.excludedFromAll = append(context.excludedFromAll, fieldName)
			}
		}
	} else if shape == geo.GeometryCollectionType {
		coordinates, shapes, found := geo.ExtractGeometryCollection(propertyMightBeGeoShape)
		if found {
			fieldName := getFieldName(pathString, path, fm)
			options := fm.Options()
			field := document.NewGeometryCollectionFieldWithIndexingOptions(fieldName,
				indexes, coordinates, shapes, options)
			context.doc.AddField(field)

			if !fm.IncludeInAll {
				context.excludedFromAll = append(context.excludedFromAll, fieldName)
			}
		}
	} else {
		coordinates, shape, found := geo.ExtractGeoShapeCoordinates(coordValue, shape)
		if found {
			fieldName := getFieldName(pathString, path, fm)
			options := fm.Options()
			field := document.NewGeoShapeFieldWithIndexingOptions(fieldName,
				indexes, coordinates, shape, options)
			context.doc.AddField(field)

			if !fm.IncludeInAll {
				context.excludedFromAll = append(context.excludedFromAll, fieldName)
			}
		}
	}
}

func (fm *FieldMapping) analyzerForField(path []string, context *walkContext) analysis.Analyzer {
	analyzerName := fm.Analyzer
	if analyzerName == "" {
		analyzerName = context.dm.defaultAnalyzerName(path)
		if analyzerName == "" {
			analyzerName = context.im.DefaultAnalyzer
		}
	}
	return context.im.AnalyzerNamed(analyzerName)
}

func getFieldName(pathString string, path []string, fieldMapping *FieldMapping) string {
	fieldName := pathString
	if fieldMapping.Name != "" {
		parentName := ""
		if len(path) > 1 {
			parentName = encodePath(path[:len(path)-1]) + pathSeparator
		}
		fieldName = parentName + fieldMapping.Name
	}
	return fieldName
}