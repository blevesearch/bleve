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
	"log"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/registry"
)

const defaultTypeField = "_type"
const defaultType = "_default"
const defaultField = "_all"
const defaultAnalyzer = "standard"
const defaultDateTimeParser = "dateTimeOptional"
const defaultByteArrayConverter = "json"

type customAnalysis struct {
	CharFilters     map[string]map[string]interface{} `json:"char_filters,omitempty"`
	Tokenizers      map[string]map[string]interface{} `json:"tokenizers,omitempty"`
	TokenMaps       map[string]map[string]interface{} `json:"token_maps,omitempty"`
	TokenFilters    map[string]map[string]interface{} `json:"token_filters,omitempty"`
	Analyzers       map[string]map[string]interface{} `json:"analyzers,omitempty"`
	DateTimeParsers map[string]map[string]interface{} `json:"date_time_parsers,omitempty"`
}

func (c *customAnalysis) registerAll(i *IndexMapping) error {
	for name, config := range c.CharFilters {
		_, err := i.cache.DefineCharFilter(name, config)
		if err != nil {
			return err
		}
	}
	for name, config := range c.Tokenizers {
		_, err := i.cache.DefineTokenizer(name, config)
		if err != nil {
			return err
		}
	}
	for name, config := range c.TokenMaps {
		_, err := i.cache.DefineTokenMap(name, config)
		if err != nil {
			return err
		}
	}
	for name, config := range c.TokenFilters {
		_, err := i.cache.DefineTokenFilter(name, config)
		if err != nil {
			return err
		}
	}
	for name, config := range c.Analyzers {
		_, err := i.cache.DefineAnalyzer(name, config)
		if err != nil {
			return err
		}
	}
	for name, config := range c.DateTimeParsers {
		_, err := i.cache.DefineDateTimeParser(name, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func newCustomAnalysis() *customAnalysis {
	rv := customAnalysis{
		CharFilters:     make(map[string]map[string]interface{}),
		Tokenizers:      make(map[string]map[string]interface{}),
		TokenMaps:       make(map[string]map[string]interface{}),
		TokenFilters:    make(map[string]map[string]interface{}),
		Analyzers:       make(map[string]map[string]interface{}),
		DateTimeParsers: make(map[string]map[string]interface{}),
	}
	return &rv
}

// An IndexMapping controls how objects are placed
// into an index.
// First the type of the object is determined.
// Once the type is know, the appropriate
// DocumentMapping is selected by the type.
// If no mapping was determined for that type,
// a DefaultMapping will be used.
type IndexMapping struct {
	TypeMapping           map[string]*DocumentMapping `json:"types,omitempty"`
	DefaultMapping        *DocumentMapping            `json:"default_mapping"`
	TypeField             string                      `json:"type_field"`
	DefaultType           string                      `json:"default_type"`
	DefaultAnalyzer       string                      `json:"default_analyzer"`
	DefaultDateTimeParser string                      `json:"default_datetime_parser"`
	DefaultField          string                      `json:"default_field"`
	ByteArrayConverter    string                      `json:"byte_array_converter"`
	CustomAnalysis        *customAnalysis             `json:"analysis,omitempty"`
	cache                 *registry.Cache
}

// AddCustomCharFilter defines a custom char filter for use in this mapping
func (im *IndexMapping) AddCustomCharFilter(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineCharFilter(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.CharFilters[name] = config
	return nil
}

// AddCustomTokenizer defines a custom tokenizer for use in this mapping
func (im *IndexMapping) AddCustomTokenizer(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenizer(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.Tokenizers[name] = config
	return nil
}

// AddCustomTokenMap defines a custom token map for use in this mapping
func (im *IndexMapping) AddCustomTokenMap(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenMap(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.TokenMaps[name] = config
	return nil
}

// AddCustomTokenFilter defines a custom token filter for use in this mapping
func (im *IndexMapping) AddCustomTokenFilter(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenFilter(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.TokenFilters[name] = config
	return nil
}

// AddCustomAnalyzer defines a custom analyzer for use in this mapping
func (im *IndexMapping) AddCustomAnalyzer(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineAnalyzer(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.Analyzers[name] = config
	return nil
}

// AddCustomDateTimeParser defines a custom date time parser for use in this mapping
func (im *IndexMapping) AddCustomDateTimeParser(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineDateTimeParser(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.DateTimeParsers[name] = config
	return nil
}

// NewIndexMapping creates a new IndexMapping that will use all the default indexing rules
func NewIndexMapping() *IndexMapping {
	return &IndexMapping{
		TypeMapping:           make(map[string]*DocumentMapping),
		DefaultMapping:        NewDocumentMapping(),
		TypeField:             defaultTypeField,
		DefaultType:           defaultType,
		DefaultAnalyzer:       defaultAnalyzer,
		DefaultDateTimeParser: defaultDateTimeParser,
		DefaultField:          defaultField,
		ByteArrayConverter:    defaultByteArrayConverter,
		CustomAnalysis:        newCustomAnalysis(),
		cache:                 registry.NewCache(),
	}
}

// Validate will walk the entire structure ensuring the following
// explicitly named and default analyzers can be built
func (im *IndexMapping) validate() error {
	_, err := im.cache.AnalyzerNamed(im.DefaultAnalyzer)
	if err != nil {
		return err
	}
	_, err = im.cache.DateTimeParserNamed(im.DefaultDateTimeParser)
	if err != nil {
		return err
	}
	err = im.DefaultMapping.validate(im.cache)
	if err != nil {
		return err
	}
	for _, docMapping := range im.TypeMapping {
		err = docMapping.validate(im.cache)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddDocumentMapping sets a custom document mapping for the specified type
func (im *IndexMapping) AddDocumentMapping(doctype string, dm *DocumentMapping) {
	im.TypeMapping[doctype] = dm
}

func (im *IndexMapping) mappingForType(docType string) *DocumentMapping {
	docMapping := im.TypeMapping[docType]
	if docMapping == nil {
		docMapping = im.DefaultMapping
	}
	return docMapping
}

// UnmarshalJSON deserializes a JSON representation of the IndexMapping
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
		CustomAnalysis        *customAnalysis             `json:"analysis"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	im.cache = registry.NewCache()

	im.CustomAnalysis = newCustomAnalysis()
	if tmp.CustomAnalysis != nil {
		if tmp.CustomAnalysis.CharFilters != nil {
			im.CustomAnalysis.CharFilters = tmp.CustomAnalysis.CharFilters
		}
		if tmp.CustomAnalysis.Tokenizers != nil {
			im.CustomAnalysis.Tokenizers = tmp.CustomAnalysis.Tokenizers
		}
		if tmp.CustomAnalysis.TokenMaps != nil {
			im.CustomAnalysis.TokenMaps = tmp.CustomAnalysis.TokenMaps
		}
		if tmp.CustomAnalysis.TokenFilters != nil {
			im.CustomAnalysis.TokenFilters = tmp.CustomAnalysis.TokenFilters
		}
		if tmp.CustomAnalysis.Analyzers != nil {
			im.CustomAnalysis.Analyzers = tmp.CustomAnalysis.Analyzers
		}
		if tmp.CustomAnalysis.DateTimeParsers != nil {
			im.CustomAnalysis.DateTimeParsers = tmp.CustomAnalysis.DateTimeParsers
		}
	}

	im.TypeField = defaultTypeField
	if tmp.TypeField != "" {
		im.TypeField = tmp.TypeField
	}

	im.DefaultType = defaultType
	if tmp.DefaultType != "" {
		im.DefaultType = tmp.DefaultType
	}

	im.DefaultAnalyzer = defaultAnalyzer
	if tmp.DefaultAnalyzer != "" {
		im.DefaultAnalyzer = tmp.DefaultAnalyzer
	}

	im.DefaultDateTimeParser = defaultDateTimeParser
	if tmp.DefaultDateTimeParser != "" {
		im.DefaultDateTimeParser = tmp.DefaultDateTimeParser
	}

	im.DefaultField = defaultField
	if tmp.DefaultField != "" {
		im.DefaultField = tmp.DefaultField
	}

	im.ByteArrayConverter = defaultByteArrayConverter
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

	err = im.CustomAnalysis.registerAll(im)
	if err != nil {
		return err
	}

	return nil
}

func (im *IndexMapping) determineType(data interface{}) string {
	// first see if the object implements Identifier
	classifier, ok := data.(Classifier)
	if ok {
		return classifier.Type()
	}

	// now see if we can find a type using the mapping
	typ, ok := mustString(lookupPropertyPath(data, im.TypeField))
	if ok {
		return typ
	}

	return im.DefaultType
}

func (im *IndexMapping) mapDocument(doc *document.Document, data interface{}) error {
	// see if the top level object is a byte array, and possibly run through a converter
	byteArrayData, ok := data.([]byte)
	if ok {
		byteArrayConverterConstructor := registry.ByteArrayConverterByName(im.ByteArrayConverter)
		if byteArrayConverterConstructor != nil {
			byteArrayConverter, err := byteArrayConverterConstructor(nil, nil)
			if err == nil {
				convertedData, err := byteArrayConverter.Convert(byteArrayData)
				if err != nil {
					return err
				}
				data = convertedData
			} else {
				log.Printf("error creating byte array converter: %v", err)
			}
		} else {
			log.Printf("no byte array converter named: %s", im.ByteArrayConverter)
		}
	}

	docType := im.determineType(data)
	docMapping := im.mappingForType(docType)
	walkContext := im.newWalkContext(doc, docMapping)
	docMapping.walkDocument(data, []string{}, []uint64{}, walkContext)

	// see if the _all field was disabled
	allMapping := docMapping.documentMappingForPath("_all")
	if allMapping == nil || (allMapping.Enabled != false) {
		field := document.NewCompositeFieldWithIndexingOptions("_all", true, []string{}, walkContext.excludedFromAll, document.IndexField|document.IncludeTermVectors)
		doc.AddField(field)
	}

	return nil
}

type walkContext struct {
	doc             *document.Document
	im              *IndexMapping
	dm              *DocumentMapping
	excludedFromAll []string
}

func (im *IndexMapping) newWalkContext(doc *document.Document, dm *DocumentMapping) *walkContext {
	return &walkContext{
		doc:             doc,
		im:              im,
		dm:              dm,
		excludedFromAll: []string{},
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
		pathMapping := docMapping.documentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != "" {
					return pathMapping.Fields[0].Analyzer
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

func (im *IndexMapping) analyzerNamed(name string) *analysis.Analyzer {
	analyzer, err := im.cache.AnalyzerNamed(name)
	if err != nil {
		log.Printf("error using analyzer named: %s", name)
		return nil
	}
	return analyzer
}

func (im *IndexMapping) dateTimeParserNamed(name string) analysis.DateTimeParser {
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
		pathMapping := docMapping.documentMappingForPath(path)
		if pathMapping != nil {
			if len(pathMapping.Fields) > 0 {
				if pathMapping.Fields[0].Analyzer != "" {
					return pathMapping.Fields[0].Analyzer
				}
			}
		}
	}

	return im.DefaultDateTimeParser
}

func (im *IndexMapping) AnalyzeText(analyzerName string, text []byte) (analysis.TokenStream, error) {
	analyzer, err := im.cache.AnalyzerNamed(analyzerName)
	if err != nil {
		return nil, err
	}
	return analyzer.Analyze(text), nil
}
