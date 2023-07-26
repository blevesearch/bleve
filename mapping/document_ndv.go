//go:build !densevector
// +build !densevector

package mapping

import (
	"fmt"
)

func validateDenseVectorField(fieldMapping *FieldMapping) error {
	return nil
}

func validateFieldType(fieldType string) error {
	switch fieldType {
	case "text", "datetime", "number", "boolean", "geopoint", "geoshape",
		"IP":
	default:
		return fmt.Errorf("unknown field type: '%s'", fieldType)
	}

	return nil
}
