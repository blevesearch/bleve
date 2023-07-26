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

//go:build densevector
// +build densevector

package mapping

import (
	"fmt"
	"reflect"
)

func validateDenseVectorField(field *FieldMapping) error {
	if field.Dims <= 0 || field.Dims > 1024 {
		return fmt.Errorf("invalid dense vector dimension,"+
			" value should be in range (%d, %d]", 0, 1024)
	}

	if field.Similarity == "" {
		field.Similarity = SimilarityDefault
	}

	// following fields are not applicable for dense vector
	// thus, we set them to default values
	field.IncludeInAll = false
	field.IncludeTermVectors = false
	field.Store = false
	field.DocValues = false

	if _, ok := SimilarityValid[field.Similarity]; !ok {
		return fmt.Errorf("invalid similarity value: '%s', "+
			"valid values are: %+v", field.Similarity,
			reflect.ValueOf(SimilarityValid).MapKeys())
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