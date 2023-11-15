//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package mapping

import (
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

func NewVectorFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "vector",
		Store:        false,
		Index:        true,
		IncludeInAll: false,
		DocValues:    false,
		SkipFreqNorm: true,
	}
}

// validate and process a flat vector
func processFlatVector(vecI interface{}, dims int) ([]float32, bool) {
	vecV := reflect.ValueOf(vecI)
	if !vecV.IsValid() || vecV.Kind() != reflect.Slice || vecV.Len() != dims {
		return nil, false
	}

	rv := make([]float32, dims)
	for i := 0; i < vecV.Len(); i++ {
		item := vecV.Index(i)
		if !item.CanInterface() {
			return nil, false
		}
		itemI := item.Interface()
		itemFloat, ok := util.ExtractNumericValFloat32(itemI)
		if !ok {
			return nil, false
		}
		rv[i] = itemFloat
	}

	return rv, true
}

// validate and process a vector
// max supported depth of nesting is 2 ([][]float32)
func processVector(vecI interface{}, dims int) ([]float32, bool) {
	vecV := reflect.ValueOf(vecI)
	if !vecV.IsValid() || vecV.Kind() != reflect.Slice || vecV.Len() == 0 {
		return nil, false
	}

	// Let's examine the first element (head) of the vector.
	// If head is a slice, then vector is nested, otherwise flat.
	headI := vecV.Index(0).Interface()
	headV := reflect.ValueOf(headI)
	if !headV.IsValid() {
		return nil, false
	}
	if headV.Kind() != reflect.Slice { // vector is flat
		return processFlatVector(vecI, dims)
	}

	// # process nested vector

	// pre-allocate memory for the flattened vector
	// so that we can use copy() later
	rv := make([]float32, dims*vecV.Len())

	for i := 0; i < vecV.Len(); i++ {
		subVec := vecV.Index(i)
		if !subVec.CanInterface() {
			return nil, false
		}
		subVecI := subVec.Interface()
		subVecV := reflect.ValueOf(subVecI)
		if !subVecV.IsValid() {
			return nil, false
		}

		if subVecV.Kind() != reflect.Slice {
			return nil, false
		}

		flatVector, ok := processFlatVector(subVecI, dims)
		if !ok {
			return nil, false
		}

		copy(rv[i*dims:(i+1)*dims], flatVector)
	}

	return rv, true
}

func (fm *FieldMapping) processVector(propertyMightBeVector interface{},
	pathString string, path []string, indexes []uint64, context *walkContext) {
	vector, ok := processVector(propertyMightBeVector, fm.Dims)
	// Don't add field to document if vector is invalid
	if !ok {
		return
	}

	fieldName := getFieldName(pathString, path, fm)
	options := fm.Options()
	field := document.NewVectorFieldWithIndexingOptions(fieldName,
		indexes, vector, fm.Dims, fm.Similarity, options)
	context.doc.AddField(field)

	// "_all" composite field is not applicable for vector field
	context.excludedFromAll = append(context.excludedFromAll, fieldName)
}

// -----------------------------------------------------------------------------
// document validation functions

func validateVectorField(field *FieldMapping) error {
	if field.Dims <= 0 || field.Dims > 2048 {
		return fmt.Errorf("invalid vector dimension,"+
			" value should be in range (%d, %d)", 0, 2048)
	}

	if field.Similarity == "" {
		field.Similarity = index.DefaultSimilarityMetric
	}

	// following fields are not applicable for vector
	// thus, we set them to default values
	field.IncludeInAll = false
	field.IncludeTermVectors = false
	field.Store = false
	field.DocValues = false
	field.SkipFreqNorm = true

	if _, ok := index.SupportedSimilarityMetrics[field.Similarity]; !ok {
		return fmt.Errorf("invalid similarity metric: '%s', "+
			"valid metrics are: %+v", field.Similarity,
			reflect.ValueOf(index.SupportedSimilarityMetrics).MapKeys())
	}

	return nil
}

func validateFieldType(fieldType string) error {
	switch fieldType {
	case "text", "datetime", "number", "boolean", "geopoint", "geoshape",
		"IP", "vector":
	default:
		return fmt.Errorf("unknown field type: '%s'", fieldType)
	}

	return nil
}
