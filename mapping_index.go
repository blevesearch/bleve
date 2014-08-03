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
	"reflect"
	"time"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/document"
)

var tRUE = true

var fALSE = false

var DEFAULT_ID_FIELD = "_id"
var DEFAULT_TYPE_FIELD = "_type"
var DEFAULT_TYPE = "_default"

type IndexMapping struct {
	TypeMapping     map[string]*DocumentMapping `json:"types"`
	DefaultMapping  *DocumentMapping            `json:"default_mapping"`
	IdField         *string                     `json:"id_field"`
	TypeField       *string                     `json:"type_field"`
	DefaultType     *string                     `json:"default_type"`
	DefaultAnalyzer *string                     `json:"default_analyzer"`
}

func (im *IndexMapping) GoString() string {
	return fmt.Sprintf("&bleve.IndexMapping{TypeMapping:%#v, TypeField:%s, DefaultType:%s}", im.TypeMapping, *im.TypeField, *im.DefaultType)
}

func NewIndexMapping() *IndexMapping {
	return &IndexMapping{
		TypeMapping:    make(map[string]*DocumentMapping),
		DefaultMapping: NewDocumentMapping(),
		IdField:        &DEFAULT_ID_FIELD,
		TypeField:      &DEFAULT_TYPE_FIELD,
		DefaultType:    &DEFAULT_TYPE,
	}
}

func (im *IndexMapping) AddDocumentMapping(doctype string, dm *DocumentMapping) *IndexMapping {
	im.TypeMapping[doctype] = dm
	return im
}

func (im *IndexMapping) SetTypeField(typeField string) *IndexMapping {
	im.TypeField = &typeField
	return im
}

func (im *IndexMapping) SetDefaultAnalyzer(analyzer string) *IndexMapping {
	im.DefaultAnalyzer = &analyzer
	return im
}

func (im *IndexMapping) MappingForType(docType string) *DocumentMapping {
	docMapping := im.TypeMapping[docType]
	if docMapping == nil {
		docMapping = im.DefaultMapping
	}
	return docMapping
}

func (im *IndexMapping) UnmarshalJSON(data []byte) error {
	var tmp struct {
		TypeMapping     map[string]*DocumentMapping `json:"types"`
		DefaultMapping  *DocumentMapping            `json:"default_mapping"`
		IdField         *string                     `json:"id_field"`
		TypeField       *string                     `json:"type_field"`
		DefaultType     *string                     `json:"default_type"`
		DefaultAnalyzer *string                     `json:"default_analyzer"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	im.IdField = &DEFAULT_ID_FIELD
	if tmp.IdField != nil {
		im.IdField = tmp.IdField
	}

	im.TypeField = &DEFAULT_TYPE_FIELD
	if tmp.TypeField != nil {
		im.TypeField = tmp.TypeField
	}

	im.DefaultType = &DEFAULT_TYPE
	if tmp.DefaultType != nil {
		im.DefaultType = tmp.DefaultType
	}

	im.DefaultMapping = NewDocumentMapping()
	if tmp.DefaultMapping != nil {
		im.DefaultMapping = tmp.DefaultMapping
	}

	if tmp.DefaultAnalyzer != nil {
		im.DefaultAnalyzer = tmp.DefaultAnalyzer
	}

	im.TypeMapping = make(map[string]*DocumentMapping, len(tmp.TypeMapping))
	for typeName, typeDocMapping := range tmp.TypeMapping {
		im.TypeMapping[typeName] = typeDocMapping
	}
	return nil
}

func (im *IndexMapping) determineType(data interface{}) (string, bool) {
	// first see if the object implements Identifier
	classifier, ok := data.(Classifier)
	if ok {
		return classifier.Type(), true
	}

	// now see if we can find type using the mapping
	if im.TypeField != nil {
		typ, ok := mustString(lookupPropertyPath(data, *im.TypeField))
		if ok {
			return typ, true
		}
	}

	// fall back to default type if there was one
	if im.DefaultType != nil {
		return *im.DefaultType, true
	}

	return "", false
}

func (im *IndexMapping) MapDocument(doc *document.Document, data interface{}) error {
	docType, ok := im.determineType(data)
	if !ok {
		return ERROR_NO_TYPE
	}
	docMapping := im.MappingForType(docType)
	walkContext := newWalkContext(doc, docMapping)
	im.walkDocument(data, []string{}, walkContext)

	// see if the _all field was disabled
	allMapping := docMapping.DocumentMappingForPath("_all")
	if allMapping == nil || (allMapping.Enabled != nil && *allMapping.Enabled != false) {
		field := document.NewCompositeFieldWithIndexingOptions("_all", true, []string{}, walkContext.excludedFromAll, document.INDEX_FIELD|document.INCLUDE_TERM_VECTORS)
		doc.AddField(field)
	}

	return nil
}

type walkContext struct {
	doc             *document.Document
	dm              *DocumentMapping
	excludedFromAll []string
}

func newWalkContext(doc *document.Document, dm *DocumentMapping) *walkContext {
	return &walkContext{
		doc:             doc,
		dm:              dm,
		excludedFromAll: []string{},
	}
}

func (im *IndexMapping) walkDocument(data interface{}, path []string, context *walkContext) {
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		// FIXME can add support for other map keys in the future
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				fieldName := key.String()
				fieldVal := val.MapIndex(key).Interface()
				im.processProperty(fieldVal, append(path, fieldName), context)
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
				im.processProperty(fieldVal, append(path, fieldName), context)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).CanInterface() {
				fieldVal := val.Index(i).Interface()
				im.processProperty(fieldVal, path, context)
			}
		}
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.CanInterface() {
			im.walkDocument(ptrElem.Interface(), path, context)
		}
	}
}

func (im *IndexMapping) processProperty(property interface{}, path []string, context *walkContext) {
	pathString := encodePath(path)
	// look to see if there is a mapping for this field
	subDocMapping := context.dm.DocumentMappingForPath(pathString)

	// check tos see if we even need to do further processing
	if subDocMapping != nil && subDocMapping.Enabled != nil && !*subDocMapping.Enabled {
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
				if *fieldMapping.Type == "text" {
					options := fieldMapping.Options()
					analyzer := Config.Analysis.Analyzers[*fieldMapping.Analyzer]
					if analyzer != nil {
						field := document.NewTextFieldCustom(fieldName, []byte(propertyValueString), options, analyzer)
						context.doc.AddField(field)

						if fieldMapping.IncludeInAll != nil && !*fieldMapping.IncludeInAll {
							context.excludedFromAll = append(context.excludedFromAll, fieldName)
						}
					}
				} else if *fieldMapping.Type == "datetime" {
					options := fieldMapping.Options()
					dateTimeFormat := *Config.DefaultDateTimeFormat
					if fieldMapping.DateFormat != nil {
						dateTimeFormat = *fieldMapping.DateFormat
					}
					dateTimeParser := Config.Analysis.DateTimeParsers[dateTimeFormat]
					parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
					if err != nil {
						field := document.NewDateTimeFieldWithIndexingOptions(fieldName, parsedDateTime, options)
						context.doc.AddField(field)
					}
				}
			}
		} else {
			// automatic indexing behavior

			// first see if it can be parsed by the default date parser
			// FIXME add support for index mapping overriding defaults
			dateTimeParser := Config.Analysis.DateTimeParsers[*Config.DefaultDateTimeFormat]
			parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
			if err != nil {
				// index as plain text
				options := document.STORE_FIELD | document.INDEX_FIELD | document.INCLUDE_TERM_VECTORS
				analyzer := im.defaultAnalyzer(context.dm, path)
				field := document.NewTextFieldCustom(pathString, []byte(propertyValueString), options, analyzer)
				context.doc.AddField(field)
			} else {
				// index as datetime
				field := document.NewDateTimeField(pathString, parsedDateTime)
				context.doc.AddField(field)
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
					field := document.NewNumericFieldWithIndexingOptions(fieldName, propertyValFloat, options)
					context.doc.AddField(field)
				}
			}
		} else {
			// automatic indexing behavior
			field := document.NewNumericField(pathString, propertyValFloat)
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
						field := document.NewDateTimeFieldWithIndexingOptions(fieldName, property, options)
						context.doc.AddField(field)
					}
				}
			} else {
				// automatic indexing behavior
				field := document.NewDateTimeField(pathString, property)
				context.doc.AddField(field)
			}

		default:
			im.walkDocument(property, path, context)
		}
	default:
		im.walkDocument(property, path, context)
	}
}

func (im *IndexMapping) defaultAnalyzer(dm *DocumentMapping, path []string) *analysis.Analyzer {
	// first see if the document mapping has an analyzer
	rv := dm.defaultAnalyzer(path)
	if rv == nil {
		if im.DefaultAnalyzer != nil {
			rv = Config.Analysis.Analyzers[*im.DefaultAnalyzer]
		} else if Config.DefaultAnalyzer != nil {
			rv = Config.Analysis.Analyzers[*Config.DefaultAnalyzer]
		}
	}
	return rv
}

// attempts to find the best analyzer to use with only a field name
// will walk all the document types, look for field mappings at the
// provided path, if one exists and it has an explicit analyzer
// that is returned
// nil should be an acceptable return value meaning we don't know
func (im *IndexMapping) analyzerForPath(path string) *analysis.Analyzer {

	// first we look for explicit mapping on the field
	for _, docMapping := range im.TypeMapping {
		pathMapping := docMapping.DocumentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != nil {
					return Config.Analysis.Analyzers[*pathMapping.Fields[0].Analyzer]
				}
			}
		}
	}

	// next we will try default analyzers for the path
	for _, docMapping := range im.TypeMapping {
		rv := im.defaultAnalyzer(docMapping, decodePath(path))
		if rv != nil {
			return rv
		}
	}

	// finally just return the system-wide default analyzer
	return Config.Analysis.Analyzers[*Config.DefaultAnalyzer]
}

func (im *IndexMapping) datetimeParserForPath(path string) analysis.DateTimeParser {

	// first we look for explicit mapping on the field
	for _, docMapping := range im.TypeMapping {
		pathMapping := docMapping.DocumentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != nil {
					return Config.Analysis.DateTimeParsers[*pathMapping.Fields[0].DateFormat]
				}
			}
		}
	}

	// next we will try default analyzers for the path
	// FIXME introduce default date time parsers at mapping leves

	// finally just return the system-wide default analyzer
	return Config.Analysis.DateTimeParsers[*Config.DefaultDateTimeFormat]
}

func getFieldName(pathString string, path []string, fieldMapping *FieldMapping) string {
	fieldName := pathString
	if fieldMapping.Name != nil && *fieldMapping.Name != "" {
		parentName := ""
		if len(path) > 1 {
			parentName = encodePath(path[:len(path)-1]) + PATH_SEPARATOR
		}
		fieldName = parentName + *fieldMapping.Name
	}
	return fieldName
}
