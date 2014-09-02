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
