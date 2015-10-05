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
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
)

// A FieldMapping describes how a specific item
// should be put into the index.
type FieldMapping struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`

	// Analyzer specifies the name of the analyzer to use for this field.  If
	// Analyzer is empty, traverse the DocumentMapping tree toward the root and
	// pick the first non-empty DefaultAnalyzer found. If there is none, use
	// the IndexMapping.DefaultAnalyzer.
	Analyzer string `json:"analyzer,omitempty"`

	// Store indicates whether to store field values in the index. Stored
	// values can be retrieved from search results using SearchRequest.Fields.
	Store              bool   `json:"store,omitempty"`
	Index              bool   `json:"index,omitempty"`
	IncludeTermVectors bool   `json:"include_term_vectors,omitempty"`
	IncludeInAll       bool   `json:"include_in_all,omitempty"`
	DateFormat         string `json:"date_format,omitempty"`
}

// NewTextFieldMapping returns a default field mapping for text
func NewTextFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:               "text",
		Store:              true,
		Index:              true,
		IncludeTermVectors: true,
		IncludeInAll:       true,
	}
}

// NewNumericFieldMapping returns a default field mapping for numbers
func NewNumericFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "number",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
	}
}

// NewDateTimeFieldMapping returns a default field mapping for dates
func NewDateTimeFieldMapping() *FieldMapping {
	return &FieldMapping{
		Type:         "datetime",
		Store:        true,
		Index:        true,
		IncludeInAll: true,
	}
}

// Options returns the indexing options for this field.
func (fm *FieldMapping) Options() document.IndexingOptions {
	var rv document.IndexingOptions
	if fm.Store {
		rv |= document.StoreField
	}
	if fm.Index {
		rv |= document.IndexField
	}
	if fm.IncludeTermVectors {
		rv |= document.IncludeTermVectors
	}
	return rv
}

func (fm *FieldMapping) processString(propertyValueString string, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	options := fm.Options()
	if fm.Type == "text" {
		analyzer := fm.analyzerForField(path, context)
		field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	} else if fm.Type == "datetime" {
		dateTimeFormat := context.im.DefaultDateTimeParser
		if fm.DateFormat != "" {
			dateTimeFormat = fm.DateFormat
		}
		dateTimeParser := context.im.dateTimeParserNamed(dateTimeFormat)
		if dateTimeParser != nil {
			parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
			if err != nil {
				fm.processTime(parsedDateTime, pathString, path, indexes, context)
			}
		}
	}
}

func (fm *FieldMapping) processFloat64(propertyValFloat float64, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "number" {
		options := fm.Options()
		field := document.NewNumericFieldWithIndexingOptions(fieldName, indexes, propertyValFloat, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processTime(propertyValueTime time.Time, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "datetime" {
		options := fm.Options()
		field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, propertyValueTime, options)
		if err == nil {
			context.doc.AddField(field)
		} else {
			logger.Printf("could not build date %v", err)
		}

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) analyzerForField(path []string, context *walkContext) *analysis.Analyzer {
	analyzerName := fm.Analyzer
	if analyzerName == "" {
		analyzerName = context.dm.defaultAnalyzerName(path)
		if analyzerName == "" {
			analyzerName = context.im.DefaultAnalyzer
		}
	}
	return context.im.analyzerNamed(analyzerName)
}

func getFieldName(pathString string, path []string, fieldMapping *FieldMapping) string {
	fieldName := pathString
	if fieldMapping.Name != "" {
		parentName := ""
		if len(path) > 1 {
			parentName = encodePath(path[:len(path)-1]) + pathSeparator
		}
		fieldName = parentName + fieldMapping.Name
	}
	return fieldName
}
