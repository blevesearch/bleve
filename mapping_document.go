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

	"github.com/couchbaselabs/bleve/analysis"
)

type DocumentMapping struct {
	Enabled         *bool                       `json:"enabled"`
	Dynamic         *bool                       `json:"dynamic"`
	Properties      map[string]*DocumentMapping `json:"properties"`
	Fields          []*FieldMapping             `json:"fields"`
	DefaultAnalyzer *string                     `json:"default_analyzer"`
}

func (dm *DocumentMapping) GoString() string {
	return fmt.Sprintf(" &bleve.DocumentMapping{Enabled:%t, Dynamic:%t, Properties:%#v, Fields:%#v}", *dm.Enabled, *dm.Dynamic, dm.Properties, dm.Fields)
}

func (dm *DocumentMapping) DocumentMappingForPath(path string) *DocumentMapping {
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

func NewDocumentMapping() *DocumentMapping {
	return &DocumentMapping{
		Enabled: &tRUE,
		Dynamic: &tRUE,
	}
}

func NewDocumentStaticMapping() *DocumentMapping {
	return &DocumentMapping{
		Enabled: &tRUE,
		Dynamic: &fALSE,
	}
}

func NewDocumentDisabledMapping() *DocumentMapping {
	return &DocumentMapping{
		Enabled: &fALSE,
	}
}

func (dm *DocumentMapping) AddSubDocumentMapping(property string, sdm *DocumentMapping) *DocumentMapping {
	if dm.Properties == nil {
		dm.Properties = make(map[string]*DocumentMapping)
	}
	dm.Properties[property] = sdm
	return dm
}

func (dm *DocumentMapping) AddFieldMapping(fm *FieldMapping) *DocumentMapping {
	if dm.Fields == nil {
		dm.Fields = make([]*FieldMapping, 0)
	}
	dm.Fields = append(dm.Fields, fm)
	return dm
}

func (dm *DocumentMapping) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Enabled         *bool                       `json:"enabled"`
		Dynamic         *bool                       `json:"dynamic"`
		Properties      map[string]*DocumentMapping `json:"properties"`
		Fields          []*FieldMapping             `json:"fields"`
		DefaultAnalyzer *string                     `json:"default_analyzer"`
	}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	dm.Enabled = &tRUE
	if tmp.Enabled != nil {
		dm.Enabled = tmp.Enabled
	}
	dm.Dynamic = &tRUE
	if tmp.Dynamic != nil {
		dm.Dynamic = tmp.Dynamic
	}
	if tmp.DefaultAnalyzer != nil {
		dm.DefaultAnalyzer = tmp.DefaultAnalyzer
	}
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

func (dm *DocumentMapping) defaultAnalyzer(path []string) *analysis.Analyzer {
	var rv *analysis.Analyzer
	current := dm
	for _, pathElement := range path {
		var ok bool
		current, ok = current.Properties[pathElement]
		if !ok {
			break
		}
		if current.DefaultAnalyzer != nil {
			rv = Config.Analysis.Analyzers[*current.DefaultAnalyzer]
		}
	}
	return rv
}
