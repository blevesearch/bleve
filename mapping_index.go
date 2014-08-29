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

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/registry"
)

var tRUE = true

var fALSE = false

const DefaultTypeField = "_type"
const DefaultType = "_default"
const DefaultField = "_all"
const DefaultAnalyzer = "standard"
const DefaultDateTimeParser = "dateTimeOptional"
const DefaultByteArrayConverter = "json"

type IndexMapping struct {
	TypeMapping           map[string]*DocumentMapping `json:"types,omitempty"`
	DefaultMapping        *DocumentMapping            `json:"default_mapping"`
	TypeField             string                      `json:"type_field"`
	DefaultType           string                      `json:"default_type"`
	DefaultAnalyzer       string                      `json:"default_analyzer"`
	DefaultDateTimeParser string                      `json:"default_datetime_parser"`
	DefaultField          string                      `json:"default_field"`
	ByteArrayConverter    string                      `json:"byte_array_converter"`
	cache                 *registry.Cache             `json:"_"`
}

func (im *IndexMapping) GoString() string {
	return fmt.Sprintf("&bleve.IndexMapping{TypeMapping:%#v, TypeField:%s, DefaultType:%s}", im.TypeMapping, im.TypeField, im.DefaultType)
}

func NewIndexMapping() *IndexMapping {
	return &IndexMapping{
		TypeMapping:           make(map[string]*DocumentMapping),
		DefaultMapping:        NewDocumentMapping(),
		TypeField:             DefaultTypeField,
		DefaultType:           DefaultType,
		DefaultAnalyzer:       DefaultAnalyzer,
		DefaultDateTimeParser: DefaultDateTimeParser,
		DefaultField:          DefaultField,
		ByteArrayConverter:    DefaultByteArrayConverter,
		cache:                 registry.NewCache(),
	}
}

// Validate will walk the entire structure ensuring the following
// explicitly named and default analyzers can be built
// explicitly named and default date parsers can be built
// field type names are valid
func (im *IndexMapping) Validate() error {
	_, err := im.cache.AnalyzerNamed(im.DefaultAnalyzer)
	if err != nil {
		return err
	}
	_, err = im.cache.DateTimeParserNamed(im.DefaultDateTimeParser)
	if err != nil {
		return err
	}
	err = im.DefaultMapping.Validate(im.cache)
	if err != nil {
		return err
	}
	for _, docMapping := range im.TypeMapping {
		err = docMapping.Validate(im.cache)
		if err != nil {
			return err
		}
	}
	return nil
}

func (im *IndexMapping) AddDocumentMapping(doctype string, dm *DocumentMapping) *IndexMapping {
	im.TypeMapping[doctype] = dm
	return im
}

func (im *IndexMapping) SetDefaultMapping(defaultMapping *DocumentMapping) *IndexMapping {
	im.DefaultMapping = defaultMapping
	return im
}

func (im *IndexMapping) SetTypeField(typeField string) *IndexMapping {
	im.TypeField = typeField
	return im
}

func (im *IndexMapping) SetDefaultType(defaultType string) *IndexMapping {
	im.DefaultType = defaultType
	return im
}

func (im *IndexMapping) SetDefaultAnalyzer(analyzer string) *IndexMapping {
	im.DefaultAnalyzer = analyzer
	return im
}

func (im *IndexMapping) SetDefaultField(field string) *IndexMapping {
	im.DefaultField = field
	return im
}

func (im *IndexMapping) SetByteArrayConverter(byteArrayConverter string) *IndexMapping {
	im.ByteArrayConverter = byteArrayConverter
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
		TypeMapping           map[string]*DocumentMapping `json:"types"`
		DefaultMapping        *DocumentMapping            `json:"default_mapping"`
		TypeField             string                      `json:"type_field"`
		DefaultType           string                      `json:"default_type"`
		DefaultAnalyzer       string                      `json:"default_analyzer"`
		DefaultDateTimeParser string                      `json:"default_datetime_parser"`
		DefaultField          string                      `json:"default_field"`
		ByteArrayConverter    string                      `json:"byte_array_converter"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	im.cache = registry.NewCache()

	im.TypeField = DefaultTypeField
	if tmp.TypeField != "" {
		im.TypeField = tmp.TypeField
	}

	im.DefaultType = DefaultType
	if tmp.DefaultType != "" {
		im.DefaultType = tmp.DefaultType
	}

	im.DefaultAnalyzer = DefaultAnalyzer
	if tmp.DefaultAnalyzer != "" {
		im.DefaultAnalyzer = tmp.DefaultAnalyzer
	}

	im.DefaultDateTimeParser = DefaultDateTimeParser
	if tmp.DefaultDateTimeParser != "" {
		im.DefaultDateTimeParser = tmp.DefaultDateTimeParser
	}

	im.DefaultField = DefaultField
	if tmp.DefaultField != "" {
		im.DefaultField = tmp.DefaultField
	}

	im.ByteArrayConverter = DefaultByteArrayConverter
	if tmp.ByteArrayConverter != "" {
		im.ByteArrayConverter = tmp.ByteArrayConverter
	}

	im.DefaultMapping = NewDocumentMapping()
	if tmp.DefaultMapping != nil {
		im.DefaultMapping = tmp.DefaultMapping
	}

	im.TypeMapping = make(map[string]*DocumentMapping, len(tmp.TypeMapping))
	for typeName, typeDocMapping := range tmp.TypeMapping {
		im.TypeMapping[typeName] = typeDocMapping
	}
	return nil
}

func (im *IndexMapping) determineType(data interface{}) string {
	// first see if the object implements Identifier
	classifier, ok := data.(Classifier)
	if ok {
		return classifier.Type()
	}

	// now see if we can find type using the mapping
	typ, ok := mustString(lookupPropertyPath(data, im.TypeField))
	if ok {
		return typ
	}

	return im.DefaultType
}

func (im *IndexMapping) MapDocument(doc *document.Document, data interface{}) error {
	// see if the top level object is a byte array, and possibly run through conveter
	byteArrayData, ok := data.([]byte)
	if ok {
		byteArrayConverter, valid := Config.ByteArrayConverters[im.ByteArrayConverter]
		if valid {
			convertedData, err := byteArrayConverter.Convert(byteArrayData)
			if err != nil {
				return err
			}
			data = convertedData
		}
	}

	docType := im.determineType(data)
	docMapping := im.MappingForType(docType)
	walkContext := newWalkContext(doc, docMapping)
	im.walkDocument(data, []string{}, []uint64{}, walkContext)

	// see if the _all field was disabled
	allMapping := docMapping.DocumentMappingForPath("_all")
	if allMapping == nil || (allMapping.Enabled != false) {
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

func (im *IndexMapping) walkDocument(data interface{}, path []string, indexes []uint64, context *walkContext) {
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		// FIXME can add support for other map keys in the future
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				fieldName := key.String()
				fieldVal := val.MapIndex(key).Interface()
				im.processProperty(fieldVal, append(path, fieldName), indexes, context)
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
				im.processProperty(fieldVal, append(path, fieldName), indexes, context)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).CanInterface() {
				fieldVal := val.Index(i).Interface()
				im.processProperty(fieldVal, path, append(indexes, uint64(i)), context)
			}
		}
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			im.walkDocument(ptrElem.Interface(), path, indexes, context)
		}
	}
}

func (im *IndexMapping) processProperty(property interface{}, path []string, indexes []uint64, context *walkContext) {
	pathString := encodePath(path)
	// look to see if there is a mapping for this field
	subDocMapping := context.dm.DocumentMappingForPath(pathString)

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
					analyzer := im.AnalyzerNamed(*fieldMapping.Analyzer)
					field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
					context.doc.AddField(field)

					if fieldMapping.IncludeInAll != nil && !*fieldMapping.IncludeInAll {
						context.excludedFromAll = append(context.excludedFromAll, fieldName)
					}
				} else if *fieldMapping.Type == "datetime" {
					dateTimeFormat := im.DefaultDateTimeParser
					if fieldMapping.DateFormat != nil {
						dateTimeFormat = *fieldMapping.DateFormat
					}
					dateTimeParser := im.DateTimeParserNamed(dateTimeFormat)
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
			dateTimeParser := im.DateTimeParserNamed(im.DefaultDateTimeParser)
			if dateTimeParser != nil {
				parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
				if err != nil {
					// index as plain text
					options := document.STORE_FIELD | document.INDEX_FIELD | document.INCLUDE_TERM_VECTORS
					analyzerName := context.dm.defaultAnalyzerName(path)
					if analyzerName == "" {
						analyzerName = im.DefaultAnalyzer
					}
					analyzer := im.AnalyzerNamed(analyzerName)
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
			im.walkDocument(property, path, indexes, context)
		}
	default:
		im.walkDocument(property, path, indexes, context)
	}
}

// attempts to find the best analyzer to use with only a field name
// will walk all the document types, look for field mappings at the
// provided path, if one exists and it has an explicit analyzer
// that is returned
// nil should be an acceptable return value meaning we don't know
func (im *IndexMapping) analyzerNameForPath(path string) string {

	// first we look for explicit mapping on the field
	for _, docMapping := range im.TypeMapping {
		pathMapping := docMapping.DocumentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != nil {
					return *pathMapping.Fields[0].Analyzer
				}
			}
		}
	}

	// next we will try default analyzers for the path
	pathDecoded := decodePath(path)
	for _, docMapping := range im.TypeMapping {
		rv := docMapping.defaultAnalyzerName(pathDecoded)
		if rv != "" {
			return rv
		}
	}

	return im.DefaultAnalyzer
}

func (im *IndexMapping) AnalyzerNamed(name string) *analysis.Analyzer {
	analyzer, err := im.cache.AnalyzerNamed(name)
	if err != nil {
		log.Printf("error using analyzer named: %s", name)
		return nil
	}
	return analyzer
}

func (im *IndexMapping) DateTimeParserNamed(name string) analysis.DateTimeParser {
	dateTimeParser, err := im.cache.DateTimeParserNamed(name)
	if err != nil {
		log.Printf("error using datetime parser named: %s", name)
		return nil
	}
	return dateTimeParser
}

func (im *IndexMapping) datetimeParserNameForPath(path string) string {

	// first we look for explicit mapping on the field
	for _, docMapping := range im.TypeMapping {
		pathMapping := docMapping.DocumentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != nil {
					return *pathMapping.Fields[0].Analyzer
				}
			}
		}
	}

	return im.DefaultDateTimeParser
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
