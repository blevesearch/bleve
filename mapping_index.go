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

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/analyzers/standard_analyzer"
	"github.com/blevesearch/bleve/analysis/datetime_parsers/datetime_optional"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/registry"
)

var MappingJSONStrict = false

const defaultTypeField = "_type"
const defaultType = "_default"
const defaultField = "_all"
const defaultAnalyzer = standard_analyzer.Name
const defaultDateTimeParser = datetime_optional.Name

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

	if len(c.Tokenizers) > 0 {
		// put all the names in map tracking work to do
		todo := map[string]struct{}{}
		for name := range c.Tokenizers {
			todo[name] = struct{}{}
		}
		registered := 1
		errs := []error{}
		// as long as we keep making progress, keep going
		for len(todo) > 0 && registered > 0 {
			registered = 0
			errs = []error{}
			for name := range todo {
				config := c.Tokenizers[name]
				_, err := i.cache.DefineTokenizer(name, config)
				if err != nil {
					errs = append(errs, err)
				} else {
					delete(todo, name)
					registered++
				}
			}
		}

		if len(errs) > 0 {
			return errs[0]
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
	StoreDynamic          bool                        `json:"store_dynamic"`
	IndexDynamic          bool                        `json:"index_dynamic"`
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

// AddCustomAnalyzer defines a custom analyzer for use in this mapping. The
// config map must have a "type" string entry to resolve the analyzer
// constructor. The constructor is invoked with the remaining entries and
// returned analyzer is registered in the IndexMapping.
//
// bleve comes with predefined analyzers, like
// github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer. They are
// available only if their package is imported by client code. To achieve this,
// use their metadata to fill configuration entries:
//
//   import (
//       "github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
//       "github.com/blevesearch/bleve/analysis/char_filters/html_char_filter"
//       "github.com/blevesearch/bleve/analysis/token_filters/lower_case_filter"
//       "github.com/blevesearch/bleve/analysis/tokenizers/unicode"
//   )
//
//   m := bleve.NewIndexMapping()
//   err := m.AddCustomAnalyzer("html", map[string]interface{}{
//       "type": custom_analyzer.Name,
//       "char_filters": []string{
//           html_char_filter.Name,
//       },
//       "tokenizer":     unicode.Name,
//       "token_filters": []string{
//           lower_case_filter.Name,
//           ...
//       },
//   })
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
		IndexDynamic:          IndexDynamic,
		StoreDynamic:          StoreDynamic,
		CustomAnalysis:        newCustomAnalysis(),
		cache:                 registry.NewCache(),
	}
}

// Validate will walk the entire structure ensuring the following
// explicitly named and default analyzers can be built
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

// UnmarshalJSON offers custom unmarshaling with optional strict validation
func (im *IndexMapping) UnmarshalJSON(data []byte) error {

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

func (im *IndexMapping) determineType(data interface{}) string {
	// first see if the object implements Classifier
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
	docType := im.determineType(data)
	docMapping := im.mappingForType(docType)
	walkContext := im.newWalkContext(doc, docMapping)
	if docMapping.Enabled {
		docMapping.walkDocument(data, []string{}, []uint64{}, walkContext)

		// see if the _all field was disabled
		allMapping := docMapping.documentMappingForPath("_all")
		if allMapping == nil || (allMapping.Enabled != false) {
			field := document.NewCompositeFieldWithIndexingOptions("_all", true, []string{}, walkContext.excludedFromAll, document.IndexField|document.IncludeTermVectors)
			doc.AddField(field)
		}
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

func (im *IndexMapping) analyzerNamed(name string) *analysis.Analyzer {
	analyzer, err := im.cache.AnalyzerNamed(name)
	if err != nil {
		logger.Printf("error using analyzer named: %s", name)
		return nil
	}
	return analyzer
}

func (im *IndexMapping) dateTimeParserNamed(name string) analysis.DateTimeParser {
	dateTimeParser, err := im.cache.DateTimeParserNamed(name)
	if err != nil {
		logger.Printf("error using datetime parser named: %s", name)
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

// FieldAnalyzer returns the name of the analyzer used on a field.
func (im *IndexMapping) FieldAnalyzer(field string) string {
	return im.analyzerNameForPath(field)
}
