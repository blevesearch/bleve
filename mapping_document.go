//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/registry"
)

// A DocumentMapping describes how a type of document
// should be indexed.
// As documents can be hierarchical, named sub-sections
// of documents are mapped using the same structure in
// the Properties field.
// Each value inside a document can be index 0 or more
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
	DefaultAnalyzer string                      `json:"default_analyzer"`
}

func (dm *DocumentMapping) validate(cache *registry.Cache) error {
	var err error
	if dm.DefaultAnalyzer != "" {
		_, err := cache.AnalyzerNamed(dm.DefaultAnalyzer)
		if err != nil {
			return err
		}
	}
	for _, property := range dm.Properties {
		err = property.validate(cache)
		if err != nil {
			return err
		}
	}
	for _, field := range dm.Fields {
		if field.Analyzer != nil {
			_, err = cache.AnalyzerNamed(*field.Analyzer)
			if err != nil {
				return err
			}
		}
		if field.DateFormat != nil {
			_, err = cache.DateTimeParserNamed(*field.DateFormat)
			if err != nil {
				return err
			}
		}
		if field.Type != nil {
			switch *field.Type {
			case "text", "datetime", "number":
			default:
				return fmt.Errorf("unknown field type: '%s'", *field.Type)
			}
		}
	}
	return nil
}

func (dm *DocumentMapping) documentMappingForPath(path string) *DocumentMapping {
	pathElements := decodePath(path)
	current := dm
	for _, pathElement := range pathElements {
		var ok bool
		current, ok = current.Properties[pathElement]
		if !ok {
			return nil
		}
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
func (dm *DocumentMapping) AddSubDocumentMapping(property string, sdm *DocumentMapping) *DocumentMapping {
	if dm.Properties == nil {
		dm.Properties = make(map[string]*DocumentMapping)
	}
	dm.Properties[property] = sdm
	return dm
}

// AddFieldMapping adds the provided FieldMapping for this section
// of the document.
func (dm *DocumentMapping) AddFieldMapping(fm *FieldMapping) *DocumentMapping {
	if dm.Fields == nil {
		dm.Fields = make([]*FieldMapping, 0)
	}
	dm.Fields = append(dm.Fields, fm)
	return dm
}

// UnmarshalJSON deserializes a JSON representation
// of the DocumentMapping.
func (dm *DocumentMapping) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Enabled         *bool                       `json:"enabled"`
		Dynamic         *bool                       `json:"dynamic"`
		Properties      map[string]*DocumentMapping `json:"properties"`
		Fields          []*FieldMapping             `json:"fields"`
		DefaultAnalyzer string                      `json:"default_analyzer"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	dm.Enabled = true
	if tmp.Enabled != nil {
		dm.Enabled = *tmp.Enabled
	}

	dm.Dynamic = true
	if tmp.Dynamic != nil {
		dm.Dynamic = *tmp.Dynamic
	}

	dm.DefaultAnalyzer = tmp.DefaultAnalyzer

	if tmp.Properties != nil {
		dm.Properties = make(map[string]*DocumentMapping, len(tmp.Properties))
	}
	for propName, propMapping := range tmp.Properties {
		dm.Properties[propName] = propMapping
	}
	if tmp.Fields != nil {
		dm.Fields = make([]*FieldMapping, len(tmp.Fields))
	}
	for i, field := range tmp.Fields {
		dm.Fields[i] = field
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
	val := reflect.ValueOf(data)
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

			// if the field has a JSON name, prefer that
			jsonTag := field.Tag.Get("json")
			jsonFieldName := parseJSONTagName(jsonTag)
			if jsonFieldName != "" {
				fieldName = jsonFieldName
			}

			if val.Field(i).CanInterface() {
				fieldVal := val.Field(i).Interface()
				dm.processProperty(fieldVal, append(path, fieldName), indexes, context)
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
			dm.walkDocument(ptrElem.Interface(), path, indexes, context)
		}
	}
}

func (dm *DocumentMapping) processProperty(property interface{}, path []string, indexes []uint64, context *walkContext) {
	pathString := encodePath(path)
	// look to see if there is a mapping for this field
	subDocMapping := dm.documentMappingForPath(pathString)

	// check tos see if we even need to do further processing
	if subDocMapping != nil && !subDocMapping.Enabled {
		return
	}

	propertyValue := reflect.ValueOf(property)
	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.String:
		propertyValueString := propertyValue.String()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldName := getFieldName(pathString, path, fieldMapping)
				options := fieldMapping.Options()
				if *fieldMapping.Type == "text" {
					analyzer := context.im.analyzerNamed(*fieldMapping.Analyzer)
					field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
					context.doc.AddField(field)

					if fieldMapping.IncludeInAll != nil && !*fieldMapping.IncludeInAll {
						context.excludedFromAll = append(context.excludedFromAll, fieldName)
					}
				} else if *fieldMapping.Type == "datetime" {
					dateTimeFormat := context.im.DefaultDateTimeParser
					if fieldMapping.DateFormat != nil {
						dateTimeFormat = *fieldMapping.DateFormat
					}
					dateTimeParser := context.im.dateTimeParserNamed(dateTimeFormat)
					if dateTimeParser != nil {
						parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
						if err != nil {
							field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, parsedDateTime, options)
							if err == nil {
								context.doc.AddField(field)
							} else {
								log.Printf("could not build date %v", err)
							}
						}
					}
				}
			}
		} else {
			// automatic indexing behavior

			// first see if it can be parsed by the default date parser
			dateTimeParser := context.im.dateTimeParserNamed(context.im.DefaultDateTimeParser)
			if dateTimeParser != nil {
				parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
				if err != nil {
					// index as plain text
					options := document.STORE_FIELD | document.INDEX_FIELD | document.INCLUDE_TERM_VECTORS
					analyzerName := dm.defaultAnalyzerName(path)
					if analyzerName == "" {
						analyzerName = context.im.DefaultAnalyzer
					}
					analyzer := context.im.analyzerNamed(analyzerName)
					field := document.NewTextFieldCustom(pathString, indexes, []byte(propertyValueString), options, analyzer)
					context.doc.AddField(field)
				} else {
					// index as datetime
					field, err := document.NewDateTimeField(pathString, indexes, parsedDateTime)
					if err == nil {
						context.doc.AddField(field)
					} else {
						log.Printf("could not build date %v", err)
					}
				}
			}
		}
	case reflect.Float64:
		propertyValFloat := propertyValue.Float()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldName := getFieldName(pathString, path, fieldMapping)
				if *fieldMapping.Type == "number" {
					options := fieldMapping.Options()
					field := document.NewNumericFieldWithIndexingOptions(fieldName, indexes, propertyValFloat, options)
					context.doc.AddField(field)
				}
			}
		} else {
			// automatic indexing behavior
			field := document.NewNumericField(pathString, indexes, propertyValFloat)
			context.doc.AddField(field)
		}
	case reflect.Struct:
		switch property := property.(type) {
		case time.Time:
			// don't descend into the time struct
			if subDocMapping != nil {
				// index by explicit mapping
				for _, fieldMapping := range subDocMapping.Fields {
					fieldName := getFieldName(pathString, path, fieldMapping)
					if *fieldMapping.Type == "datetime" {
						options := fieldMapping.Options()
						field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, property, options)
						if err == nil {
							context.doc.AddField(field)
						} else {
							log.Printf("could not build date %v", err)
						}
					}
				}
			} else {
				// automatic indexing behavior
				field, err := document.NewDateTimeField(pathString, indexes, property)
				if err == nil {
					context.doc.AddField(field)
				} else {
					log.Printf("could not build date %v", err)
				}
			}
		default:
			dm.walkDocument(property, path, indexes, context)
		}
	default:
		dm.walkDocument(property, path, indexes, context)
	}
}
