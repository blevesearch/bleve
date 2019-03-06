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
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/blevesearch/bleve/registry"
)

// A DocumentMapping describes how a type of document
// should be indexed.
// As documents can be hierarchical, named sub-sections
// of documents are mapped using the same structure in
// the Properties field.
// Each value inside a document can be indexed 0 or more
// ways.  These index entries are called fields and
// are stored in the Fields field.
// Entire sections of a document can be ignored or
// excluded by setting Enabled to false.
// If not explicitly mapped, default mapping operations
// are used.  To disable this automatic handling, set
// Dynamic to false.
type DocumentMapping struct {
	Enabled         bool                        `json:"enabled"`
	Dynamic         bool                        `json:"dynamic"`
	Properties      map[string]*DocumentMapping `json:"properties,omitempty"`
	Fields          []*FieldMapping             `json:"fields,omitempty"`
	DefaultAnalyzer string                      `json:"default_analyzer,omitempty"`

	// StructTagKey overrides "json" when looking for field names in struct tags
	StructTagKey string `json:"struct_tag_key,omitempty"`
}

func (dm *DocumentMapping) Validate(cache *registry.Cache) error {
	var err error
	if dm.DefaultAnalyzer != "" {
		_, err := cache.AnalyzerNamed(dm.DefaultAnalyzer)
		if err != nil {
			return err
		}
	}
	for _, property := range dm.Properties {
		err = property.Validate(cache)
		if err != nil {
			return err
		}
	}
	for _, field := range dm.Fields {
		if field.Analyzer != "" {
			_, err = cache.AnalyzerNamed(field.Analyzer)
			if err != nil {
				return err
			}
		}
		if field.DateFormat != "" {
			_, err = cache.DateTimeParserNamed(field.DateFormat)
			if err != nil {
				return err
			}
		}
		switch field.Type {
		case "text", "datetime", "number", "boolean", "geopoint":
		default:
			return fmt.Errorf("unknown field type: '%s'", field.Type)
		}
	}
	return nil
}

// analyzerNameForPath attempts to first find the field
// described by this path, then returns the analyzer
// configured for that field
func (dm *DocumentMapping) analyzerNameForPath(path string) string {
	field := dm.fieldDescribedByPath(path)
	if field != nil {
		return field.Analyzer
	}
	return ""
}

func (dm *DocumentMapping) fieldDescribedByPath(path string) *FieldMapping {
	pathElements := decodePath(path)
	if len(pathElements) > 1 {
		// easy case, there is more than 1 path element remaining
		// the next path element must match a property name
		// at this level
		for propName, subDocMapping := range dm.Properties {
			if propName == pathElements[0] {
				return subDocMapping.fieldDescribedByPath(encodePath(pathElements[1:]))
			}
		}
	} else {
		// just 1 path elememnt
		// first look for property name with empty field
		for propName, subDocMapping := range dm.Properties {
			if propName == pathElements[0] {
				// found property name match, now look at its fields
				for _, field := range subDocMapping.Fields {
					if field.Name == "" || field.Name == pathElements[0] {
						// match
						return field
					}
				}
			}
		}
		// next, walk the properties again, looking for field overriding the name
		for propName, subDocMapping := range dm.Properties {
			if propName != pathElements[0] {
				// property name isn't a match, but field name could override it
				for _, field := range subDocMapping.Fields {
					if field.Name == pathElements[0] {
						return field
					}
				}
			}
		}
	}

	return nil
}

// documentMappingForPath only returns EXACT matches for a sub document
// or for an explicitly mapped field, if you want to find the
// closest document mapping to a field not explicitly mapped
// use closestDocMapping
func (dm *DocumentMapping) documentMappingForPath(path string) *DocumentMapping {
	pathElements := decodePath(path)
	current := dm
OUTER:
	for i, pathElement := range pathElements {
		for name, subDocMapping := range current.Properties {
			if name == pathElement {
				current = subDocMapping
				continue OUTER
			}
		}
		// no subDocMapping matches this pathElement
		// only if this is the last element check for field name
		if i == len(pathElements)-1 {
			for _, field := range current.Fields {
				if field.Name == pathElement {
					break
				}
			}
		}

		return nil
	}
	return current
}

// closestDocMapping findest the most specific document mapping that matches
// part of the provided path
func (dm *DocumentMapping) closestDocMapping(path string) *DocumentMapping {
	pathElements := decodePath(path)
	current := dm
OUTER:
	for _, pathElement := range pathElements {
		for name, subDocMapping := range current.Properties {
			if name == pathElement {
				current = subDocMapping
				continue OUTER
			}
		}
		break
	}
	return current
}

// NewDocumentMapping returns a new document mapping
// with all the default values.
func NewDocumentMapping() *DocumentMapping {
	return &DocumentMapping{
		Enabled: true,
		Dynamic: true,
	}
}

// NewDocumentStaticMapping returns a new document
// mapping that will not automatically index parts
// of a document without an explicit mapping.
func NewDocumentStaticMapping() *DocumentMapping {
	return &DocumentMapping{
		Enabled: true,
	}
}

// NewDocumentDisabledMapping returns a new document
// mapping that will not perform any indexing.
func NewDocumentDisabledMapping() *DocumentMapping {
	return &DocumentMapping{}
}

// AddSubDocumentMapping adds the provided DocumentMapping as a sub-mapping
// for the specified named subsection.
func (dm *DocumentMapping) AddSubDocumentMapping(property string, sdm *DocumentMapping) {
	if dm.Properties == nil {
		dm.Properties = make(map[string]*DocumentMapping)
	}
	dm.Properties[property] = sdm
}

// AddFieldMappingsAt adds one or more FieldMappings
// at the named sub-document.  If the named sub-document
// doesn't yet exist it is created for you.
// This is a convenience function to make most common
// mappings more concise.
// Otherwise, you would:
//     subMapping := NewDocumentMapping()
//     subMapping.AddFieldMapping(fieldMapping)
//     parentMapping.AddSubDocumentMapping(property, subMapping)
func (dm *DocumentMapping) AddFieldMappingsAt(property string, fms ...*FieldMapping) {
	if dm.Properties == nil {
		dm.Properties = make(map[string]*DocumentMapping)
	}
	sdm, ok := dm.Properties[property]
	if !ok {
		sdm = NewDocumentMapping()
	}
	for _, fm := range fms {
		sdm.AddFieldMapping(fm)
	}
	dm.Properties[property] = sdm
}

// AddFieldMapping adds the provided FieldMapping for this section
// of the document.
func (dm *DocumentMapping) AddFieldMapping(fm *FieldMapping) {
	if dm.Fields == nil {
		dm.Fields = make([]*FieldMapping, 0)
	}
	dm.Fields = append(dm.Fields, fm)
}

// UnmarshalJSON offers custom unmarshaling with optional strict validation
func (dm *DocumentMapping) UnmarshalJSON(data []byte) error {

	var tmp map[string]json.RawMessage
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	// set defaults for fields which might have been omitted
	dm.Enabled = true
	dm.Dynamic = true

	var invalidKeys []string
	for k, v := range tmp {
		switch k {
		case "enabled":
			err := json.Unmarshal(v, &dm.Enabled)
			if err != nil {
				return err
			}
		case "dynamic":
			err := json.Unmarshal(v, &dm.Dynamic)
			if err != nil {
				return err
			}
		case "default_analyzer":
			err := json.Unmarshal(v, &dm.DefaultAnalyzer)
			if err != nil {
				return err
			}
		case "properties":
			err := json.Unmarshal(v, &dm.Properties)
			if err != nil {
				return err
			}
		case "fields":
			err := json.Unmarshal(v, &dm.Fields)
			if err != nil {
				return err
			}
		case "struct_tag_key":
			err := json.Unmarshal(v, &dm.StructTagKey)
			if err != nil {
				return err
			}
		default:
			invalidKeys = append(invalidKeys, k)
		}
	}

	if MappingJSONStrict && len(invalidKeys) > 0 {
		return fmt.Errorf("document mapping contains invalid keys: %v", invalidKeys)
	}

	return nil
}

func (dm *DocumentMapping) defaultAnalyzerName(path []string) string {
	rv := ""
	current := dm
	for _, pathElement := range path {
		var ok bool
		current, ok = current.Properties[pathElement]
		if !ok {
			break
		}
		if current.DefaultAnalyzer != "" {
			rv = current.DefaultAnalyzer
		}
	}
	return rv
}

func (dm *DocumentMapping) walkDocument(data interface{}, path []string, indexes []uint64, context *walkContext) {
	// allow default "json" tag to be overridden
	structTagKey := dm.StructTagKey
	if structTagKey == "" {
		structTagKey = "json"
	}

	val := reflect.ValueOf(data)
	if !val.IsValid() {
		return
	}

	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		// FIXME can add support for other map keys in the future
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				fieldName := key.String()
				fieldVal := val.MapIndex(key).Interface()
				dm.processProperty(fieldVal, append(path, fieldName), indexes, context)
			}
		}
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			field := typ.Field(i)
			fieldName := field.Name
			// anonymous fields of type struct can elide the type name
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				fieldName = ""
			}

			// if the field has a name under the specified tag, prefer that
			tag := field.Tag.Get(structTagKey)
			tagFieldName := parseTagName(tag)
			if tagFieldName == "-" {
				continue
			}
			// allow tag to set field name to empty, only if anonymous
			if field.Tag != "" && (tagFieldName != "" || field.Anonymous) {
				fieldName = tagFieldName
			}

			if val.Field(i).CanInterface() {
				fieldVal := val.Field(i).Interface()
				newpath := path
				if fieldName != "" {
					newpath = append(path, fieldName)
				}
				dm.processProperty(fieldVal, newpath, indexes, context)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).CanInterface() {
				fieldVal := val.Index(i).Interface()
				dm.processProperty(fieldVal, path, append(indexes, uint64(i)), context)
			}
		}
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			dm.processProperty(ptrElem.Interface(), path, indexes, context)
		}
	case reflect.String:
		dm.processProperty(val.String(), path, indexes, context)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dm.processProperty(float64(val.Int()), path, indexes, context)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dm.processProperty(float64(val.Uint()), path, indexes, context)
	case reflect.Float32, reflect.Float64:
		dm.processProperty(float64(val.Float()), path, indexes, context)
	case reflect.Bool:
		dm.processProperty(val.Bool(), path, indexes, context)
	}

}

func (dm *DocumentMapping) processProperty(property interface{}, path []string, indexes []uint64, context *walkContext) {
	pathString := encodePath(path)
	// look to see if there is a mapping for this field
	subDocMapping := dm.documentMappingForPath(pathString)
	closestDocMapping := dm.closestDocMapping(pathString)

	// check to see if we even need to do further processing
	if subDocMapping != nil && !subDocMapping.Enabled {
		return
	}

	propertyValue := reflect.ValueOf(property)
	if !propertyValue.IsValid() {
		// cannot do anything with the zero value
		return
	}
	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.String:
		propertyValueString := propertyValue.String()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				if fieldMapping.Type == "geopoint" {
					fieldMapping.processGeoPoint(property, pathString, path, indexes, context)
				} else {
					fieldMapping.processString(propertyValueString, pathString, path, indexes, context)
				}
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior

			// first see if it can be parsed by the default date parser
			dateTimeParser := context.im.DateTimeParserNamed(context.im.DefaultDateTimeParser)
			if dateTimeParser != nil {
				parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
				if err != nil {
					// index as text
					fieldMapping := newTextFieldMappingDynamic(context.im)
					fieldMapping.processString(propertyValueString, pathString, path, indexes, context)
				} else {
					// index as datetime
					fieldMapping := newDateTimeFieldMappingDynamic(context.im)
					fieldMapping.processTime(parsedDateTime, pathString, path, indexes, context)
				}
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dm.processProperty(float64(propertyValue.Int()), path, indexes, context)
		return
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dm.processProperty(float64(propertyValue.Uint()), path, indexes, context)
		return
	case reflect.Float64, reflect.Float32:
		propertyValFloat := propertyValue.Float()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldMapping.processFloat64(propertyValFloat, pathString, path, indexes, context)
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior
			fieldMapping := newNumericFieldMappingDynamic(context.im)
			fieldMapping.processFloat64(propertyValFloat, pathString, path, indexes, context)
		}
	case reflect.Bool:
		propertyValBool := propertyValue.Bool()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldMapping.processBoolean(propertyValBool, pathString, path, indexes, context)
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior
			fieldMapping := newBooleanFieldMappingDynamic(context.im)
			fieldMapping.processBoolean(propertyValBool, pathString, path, indexes, context)
		}
	case reflect.Struct:
		switch property := property.(type) {
		case time.Time:
			// don't descend into the time struct
			if subDocMapping != nil {
				// index by explicit mapping
				for _, fieldMapping := range subDocMapping.Fields {
					fieldMapping.processTime(property, pathString, path, indexes, context)
				}
			} else if closestDocMapping.Dynamic {
				fieldMapping := newDateTimeFieldMappingDynamic(context.im)
				fieldMapping.processTime(property, pathString, path, indexes, context)
			}
		case encoding.TextMarshaler:
			txt, err := property.MarshalText()
			if err == nil && subDocMapping != nil {
				// index by explicit mapping
				for _, fieldMapping := range subDocMapping.Fields {
					if fieldMapping.Type == "text" {
						fieldMapping.processString(string(txt), pathString, path, indexes, context)
					}
				}
			}
			dm.walkDocument(property, path, indexes, context)
		default:
			if subDocMapping != nil {
				for _, fieldMapping := range subDocMapping.Fields {
					if fieldMapping.Type == "geopoint" {
						fieldMapping.processGeoPoint(property, pathString, path, indexes, context)
					}
				}
			}
			dm.walkDocument(property, path, indexes, context)
		}
	case reflect.Map, reflect.Slice:
		if subDocMapping != nil {
			for _, fieldMapping := range subDocMapping.Fields {
				if fieldMapping.Type == "geopoint" {
					fieldMapping.processGeoPoint(property, pathString, path, indexes, context)
				}
			}
		}
		dm.walkDocument(property, path, indexes, context)
	case reflect.Ptr:
		if !propertyValue.IsNil() {
			switch property := property.(type) {
			case encoding.TextMarshaler:
				// ONLY process TextMarshaler if there is an explicit mapping
				// AND all of the fiels are of type text
				// OTHERWISE process field without TextMarshaler
				if subDocMapping != nil {
					allFieldsText := true
					for _, fieldMapping := range subDocMapping.Fields {
						if fieldMapping.Type != "text" {
							allFieldsText = false
							break
						}
					}
					txt, err := property.MarshalText()
					if err == nil && allFieldsText {
						txtStr := string(txt)
						for _, fieldMapping := range subDocMapping.Fields {
							fieldMapping.processString(txtStr, pathString, path, indexes, context)
						}
						return
					}
				}
				dm.walkDocument(property, path, indexes, context)
			default:
				dm.walkDocument(property, path, indexes, context)
			}
		}
	default:
		dm.walkDocument(property, path, indexes, context)
	}
}
