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
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/analysis/datetime/optional"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/registry"
)

var MappingJSONStrict = false

const defaultTypeField = "_type"
const defaultType = "_default"
const defaultField = "_all"
const defaultAnalyzer = standard.Name
const defaultDateTimeParser = optional.Name

// An IndexMappingImpl controls how objects are placed
// into an index.
// First the type of the object is determined.
// Once the type is know, the appropriate
// DocumentMapping is selected by the type.
// If no mapping was determined for that type,
// a DefaultMapping will be used.
type IndexMappingImpl struct {
	TypeMapping           map[string]*DocumentMapping `json:"types,omitempty"`
	DefaultMapping        *DocumentMapping            `json:"default_mapping"`
	TypeField             string                      `json:"type_field"`
	DefaultType           string                      `json:"default_type"`
	DefaultAnalyzer       string                      `json:"default_analyzer"`
	DefaultDateTimeParser string                      `json:"default_datetime_parser"`
	DefaultField          string                      `json:"default_field"`
	StoreDynamic          bool                        `json:"store_dynamic"`
	IndexDynamic          bool                        `json:"index_dynamic"`
	DocValuesDynamic      bool                        `json:"docvalues_dynamic"`
	CustomAnalysis        *customAnalysis             `json:"analysis,omitempty"`
	cache                 *registry.Cache
}

// AddCustomCharFilter defines a custom char filter for use in this mapping
func (im *IndexMappingImpl) AddCustomCharFilter(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineCharFilter(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.CharFilters[name] = config
	return nil
}

// AddCustomTokenizer defines a custom tokenizer for use in this mapping
func (im *IndexMappingImpl) AddCustomTokenizer(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenizer(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.Tokenizers[name] = config
	return nil
}

// AddCustomTokenMap defines a custom token map for use in this mapping
func (im *IndexMappingImpl) AddCustomTokenMap(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenMap(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.TokenMaps[name] = config
	return nil
}

// AddCustomTokenFilter defines a custom token filter for use in this mapping
func (im *IndexMappingImpl) AddCustomTokenFilter(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineTokenFilter(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.TokenFilters[name] = config
	return nil
}

// AddCustomAnalyzer defines a custom analyzer for use in this mapping. The
// config map must have a "type" string entry to resolve the analyzer
// constructor. The constructor is invoked with the remaining entries and
// returned analyzer is registered in the IndexMapping.
//
// bleve comes with predefined analyzers, like
// github.com/blevesearch/bleve/analysis/analyzer/custom. They are
// available only if their package is imported by client code. To achieve this,
// use their metadata to fill configuration entries:
//
//   import (
//       "github.com/blevesearch/bleve/analysis/analyzer/custom"
//       "github.com/blevesearch/bleve/analysis/char/html"
//       "github.com/blevesearch/bleve/analysis/token/lowercase"
//       "github.com/blevesearch/bleve/analysis/tokenizer/unicode"
//   )
//
//   m := bleve.NewIndexMapping()
//   err := m.AddCustomAnalyzer("html", map[string]interface{}{
//       "type": custom.Name,
//       "char_filters": []string{
//           html.Name,
//       },
//       "tokenizer":     unicode.Name,
//       "token_filters": []string{
//           lowercase.Name,
//           ...
//       },
//   })
func (im *IndexMappingImpl) AddCustomAnalyzer(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineAnalyzer(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.Analyzers[name] = config
	return nil
}

// AddCustomDateTimeParser defines a custom date time parser for use in this mapping
func (im *IndexMappingImpl) AddCustomDateTimeParser(name string, config map[string]interface{}) error {
	_, err := im.cache.DefineDateTimeParser(name, config)
	if err != nil {
		return err
	}
	im.CustomAnalysis.DateTimeParsers[name] = config
	return nil
}

// NewIndexMapping creates a new IndexMapping that will use all the default indexing rules
func NewIndexMapping() *IndexMappingImpl {
	return &IndexMappingImpl{
		TypeMapping:           make(map[string]*DocumentMapping),
		DefaultMapping:        NewDocumentMapping(),
		TypeField:             defaultTypeField,
		DefaultType:           defaultType,
		DefaultAnalyzer:       defaultAnalyzer,
		DefaultDateTimeParser: defaultDateTimeParser,
		DefaultField:          defaultField,
		IndexDynamic:          IndexDynamic,
		StoreDynamic:          StoreDynamic,
		DocValuesDynamic:      DocValuesDynamic,
		CustomAnalysis:        newCustomAnalysis(),
		cache:                 registry.NewCache(),
	}
}

// Validate will walk the entire structure ensuring the following
// explicitly named and default analyzers can be built
func (im *IndexMappingImpl) Validate() error {
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

// AddDocumentMapping sets a custom document mapping for the specified type
func (im *IndexMappingImpl) AddDocumentMapping(doctype string, dm *DocumentMapping) {
	im.TypeMapping[doctype] = dm
}

func (im *IndexMappingImpl) mappingForType(docType string) *DocumentMapping {
	docMapping := im.TypeMapping[docType]
	if docMapping == nil {
		docMapping = im.DefaultMapping
	}
	return docMapping
}

// UnmarshalJSON offers custom unmarshaling with optional strict validation
func (im *IndexMappingImpl) UnmarshalJSON(data []byte) error {

	var tmp map[string]json.RawMessage
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	// set defaults for fields which might have been omitted
	im.cache = registry.NewCache()
	im.CustomAnalysis = newCustomAnalysis()
	im.TypeField = defaultTypeField
	im.DefaultType = defaultType
	im.DefaultAnalyzer = defaultAnalyzer
	im.DefaultDateTimeParser = defaultDateTimeParser
	im.DefaultField = defaultField
	im.DefaultMapping = NewDocumentMapping()
	im.TypeMapping = make(map[string]*DocumentMapping)
	im.StoreDynamic = StoreDynamic
	im.IndexDynamic = IndexDynamic
	im.DocValuesDynamic = DocValuesDynamic

	var invalidKeys []string
	for k, v := range tmp {
		switch k {
		case "analysis":
			err := json.Unmarshal(v, &im.CustomAnalysis)
			if err != nil {
				return err
			}
		case "type_field":
			err := json.Unmarshal(v, &im.TypeField)
			if err != nil {
				return err
			}
		case "default_type":
			err := json.Unmarshal(v, &im.DefaultType)
			if err != nil {
				return err
			}
		case "default_analyzer":
			err := json.Unmarshal(v, &im.DefaultAnalyzer)
			if err != nil {
				return err
			}
		case "default_datetime_parser":
			err := json.Unmarshal(v, &im.DefaultDateTimeParser)
			if err != nil {
				return err
			}
		case "default_field":
			err := json.Unmarshal(v, &im.DefaultField)
			if err != nil {
				return err
			}
		case "default_mapping":
			err := json.Unmarshal(v, &im.DefaultMapping)
			if err != nil {
				return err
			}
		case "types":
			err := json.Unmarshal(v, &im.TypeMapping)
			if err != nil {
				return err
			}
		case "store_dynamic":
			err := json.Unmarshal(v, &im.StoreDynamic)
			if err != nil {
				return err
			}
		case "index_dynamic":
			err := json.Unmarshal(v, &im.IndexDynamic)
			if err != nil {
				return err
			}
		case "docvalues_dynamic":
			err := json.Unmarshal(v, &im.DocValuesDynamic)
			if err != nil {
				return err
			}
		default:
			invalidKeys = append(invalidKeys, k)
		}
	}

	if MappingJSONStrict && len(invalidKeys) > 0 {
		return fmt.Errorf("index mapping contains invalid keys: %v", invalidKeys)
	}

	err = im.CustomAnalysis.registerAll(im)
	if err != nil {
		return err
	}

	return nil
}

func (im *IndexMappingImpl) determineType(data interface{}) string {
	// first see if the object implements bleveClassifier
	bleveClassifier, ok := data.(bleveClassifier)
	if ok {
		return bleveClassifier.BleveType()
	}
	// next see if the object implements Classifier
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

func (im *IndexMappingImpl) MapDocument(doc *document.Document, data interface{}) error {
	docType := im.determineType(data)
	docMapping := im.mappingForType(docType)
	if docMapping.Enabled {
		walkContext := im.newWalkContext(doc, docMapping)
		docMapping.walkDocument(data, []string{}, []uint64{}, walkContext)

		// see if the _all field was disabled
		allMapping := docMapping.documentMappingForPath("_all")
		if allMapping == nil || allMapping.Enabled {
			field := document.NewCompositeFieldWithIndexingOptions("_all", true, []string{}, walkContext.excludedFromAll, document.IndexField|document.IncludeTermVectors)
			doc.AddField(field)
		}
	}

	return nil
}

type walkContext struct {
	doc             *document.Document
	im              *IndexMappingImpl
	dm              *DocumentMapping
	excludedFromAll []string
}

func (im *IndexMappingImpl) newWalkContext(doc *document.Document, dm *DocumentMapping) *walkContext {
	return &walkContext{
		doc:             doc,
		im:              im,
		dm:              dm,
		excludedFromAll: []string{"_id"},
	}
}

// AnalyzerNameForPath attempts to find the best analyzer to use with only a
// field name will walk all the document types, look for field mappings at the
// provided path, if one exists and it has an explicit analyzer that is
// returned.
func (im *IndexMappingImpl) AnalyzerNameForPath(path string) string {
	// first we look for explicit mapping on the field
	for _, docMapping := range im.TypeMapping {
		analyzerName := docMapping.analyzerNameForPath(path)
		if analyzerName != "" {
			return analyzerName
		}
	}
	// now try the default mapping
	pathMapping := im.DefaultMapping.documentMappingForPath(path)
	if pathMapping != nil {
		if len(pathMapping.Fields) > 0 {
			if pathMapping.Fields[0].Analyzer != "" {
				return pathMapping.Fields[0].Analyzer
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

func (im *IndexMappingImpl) AnalyzerNamed(name string) *analysis.Analyzer {
	analyzer, err := im.cache.AnalyzerNamed(name)
	if err != nil {
		logger.Printf("error using analyzer named: %s", name)
		return nil
	}
	return analyzer
}

func (im *IndexMappingImpl) DateTimeParserNamed(name string) analysis.DateTimeParser {
	if name == "" {
		name = im.DefaultDateTimeParser
	}
	dateTimeParser, err := im.cache.DateTimeParserNamed(name)
	if err != nil {
		logger.Printf("error using datetime parser named: %s", name)
		return nil
	}
	return dateTimeParser
}

func (im *IndexMappingImpl) datetimeParserNameForPath(path string) string {

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

func (im *IndexMappingImpl) AnalyzeText(analyzerName string, text []byte) (analysis.TokenStream, error) {
	analyzer, err := im.cache.AnalyzerNamed(analyzerName)
	if err != nil {
		return nil, err
	}
	return analyzer.Analyze(text), nil
}

// FieldAnalyzer returns the name of the analyzer used on a field.
func (im *IndexMappingImpl) FieldAnalyzer(field string) string {
	return im.AnalyzerNameForPath(field)
}

// wrapper to satisfy new interface

func (im *IndexMappingImpl) DefaultSearchField() string {
	return im.DefaultField
}
