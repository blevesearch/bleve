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

func (fm *FieldMapping) processVector(propertyMightBeVector interface{},
	pathString string, path []string, indexes []uint64, context *walkContext) {
	propertyVal := reflect.ValueOf(propertyMightBeVector)
	if !propertyVal.IsValid() {
		return
	}

	// Validating the length of the vector is required here, in order to
	// help zapx in deciding the shape of the batch of vectors to be indexed.
	if propertyVal.Kind() == reflect.Slice && propertyVal.Len() == fm.Dims {
		vector := make([]float32, propertyVal.Len())
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
				vector[i] = itemFloat
			}
		}
		// Even if one of the vector elements is not a float32, we do not index
		// this field and return silently
		if !isVectorValid {
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
}

// -----------------------------------------------------------------------------
// document validation functions

func validateFieldMapping(field *FieldMapping, parentName string,
	fieldAliasCtx map[string]*FieldMapping) error {
	switch field.Type {
	case "vector":
		return validateVectorFieldAlias(field, parentName, fieldAliasCtx)
	default:
		err := validateFieldType(field)
		if err != nil {
			return err
		}
		return validateFieldAlias(field, parentName, fieldAliasCtx)
	}
}

func validateVectorFieldAlias(field *FieldMapping, parentName string,
	fieldAliasCtx map[string]*FieldMapping) error {

	if field.Name == "" {
		field.Name = parentName
	}
	if field.Similarity == "" {
		field.Similarity = util.DefaultSimilarityMetric
	}

	// following fields are not applicable for vector
	// thus, we set them to default values
	field.IncludeInAll = false
	field.IncludeTermVectors = false
	field.Store = false
	field.DocValues = false
	field.SkipFreqNorm = true

	// If alias is present, validate the field as per the alias
	if fieldAlias, ok := fieldAliasCtx[field.Name]; ok {
		if field.Dims != fieldAlias.Dims {
			return fmt.Errorf("field: '%s', invalid alias "+
				"(different dimensions %d and %d)", fieldAlias.Name, field.Dims,
				fieldAlias.Dims)
		}

		if field.Similarity != fieldAlias.Similarity {
			return fmt.Errorf("field: '%s', invalid alias "+
				"(different similarity values %s and %s)", fieldAlias.Name,
				field.Similarity, fieldAlias.Similarity)
		}

		return nil
	}

	if field.Dims <= 0 || field.Dims > 2048 {
		return fmt.Errorf("field: '%s', invalid vector dimension: %d,"+
			" value should be in range (%d, %d)", field.Name, field.Dims, 0, 2048)
	}

	if _, ok := util.SupportedSimilarityMetrics[field.Similarity]; !ok {
		return fmt.Errorf("field: '%s', invalid similarity "+
			"metric: '%s', valid metrics are: %+v", field.Name, field.Similarity,
			reflect.ValueOf(util.SupportedSimilarityMetrics).MapKeys())
	}

	fieldAliasCtx[field.Name] = field

	return nil
}
