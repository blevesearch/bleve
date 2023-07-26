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