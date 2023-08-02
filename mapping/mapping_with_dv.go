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

//go:build densevector
// +build densevector

package mapping

import (
	"fmt"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	"github.com/davecgh/go-spew/spew"
	"reflect"
)

// Default Similarity value for dense_vector fields
const SimilarityDefaultVal = "l2_norm"

// Set of Valid Similarity values for dense_vector fields
var SimilarityValidVals = map[string]struct{}{
	"l2_norm":     {},
	"cosine":      {},
	"dot_product": {},
}

func NewDenseVectorFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "densevector",
		Store:        false,
		Index:        true,
		IncludeInAll: false,
		DocValues:    false,
		SkipFreqNorm: true,
	}
}

func (fm *FieldMapping) processDenseVector(propertyMightBeDenseVector interface{},
	pathString string, path []string, indexes []uint64, context *walkContext) {
	propertyVal := reflect.ValueOf(propertyMightBeDenseVector)
	if !propertyVal.IsValid() {
		return
	}

	// Validating the length of the vector is required here, in order to
	// help zapx in deciding the shape of the batch of vectors to be indexed.
	if propertyVal.Kind() == reflect.Slice && propertyVal.Len() == fm.Dims {
		dv := make([]float32, propertyVal.Len())
		isVectorValid := true
		for i := 0; i < propertyVal.Len(); i++ {
			item := propertyVal.Index(i)
			if item.CanInterface() {
				itemVal := item.Interface()
				itemFloat, ok := util.ExtractNumericValFloat32(itemVal)
				if !ok {
					isVectorValid = false
					break
				}
				dv[i] = itemFloat
			}
		}
		// Even if one of the vector elements is not a float32, we do not index
		// this field and return silently
		if !isVectorValid {
			return
		}

		fieldName := getFieldName(pathString, path, fm)
		options := fm.Options()
		field := document.NewDenseVectorFieldWithIndexingOptions(fieldName,
			indexes, dv, fm.Dims, fm.Similarity, options)
		context.doc.AddField(field)

		// "_all" composite field is not applicable for dense vector field
		context.excludedFromAll = append(context.excludedFromAll, fieldName)
	}
}

// -----------------------------------------------------------------------------
// document validation functions

func validateDenseVectorField(field *FieldMapping) error {
	if field.Dims <= 0 || field.Dims > 1024 {
		return fmt.Errorf("invalid dense vector dimension,"+
			" value should be in range (%d, %d]", 0, 1024)
	}

	if field.Similarity == "" {
		field.Similarity = SimilarityDefaultVal
	}

	// following fields are not applicable for dense vector
	// thus, we set them to default values
	field.IncludeInAll = false
	field.IncludeTermVectors = false
	field.Store = false
	field.DocValues = false
	field.SkipFreqNorm = true

	if _, ok := SimilarityValidVals[field.Similarity]; !ok {
		return fmt.Errorf("invalid similarity value: '%s', "+
			"valid values are: %+v", field.Similarity,
			reflect.ValueOf(SimilarityValidVals).MapKeys())
	}

	return nil
}

func validateFieldType(fieldType string) error {
	switch fieldType {
	case "text", "datetime", "number", "boolean", "geopoint", "geoshape",
		"IP", "densevector":
	default:
		return fmt.Errorf("unknown field type: '%s'", fieldType)
	}

	return nil
}
