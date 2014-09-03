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
	"log"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
)

// A FieldMapping describes how a specific item
// should be put into the index.
type FieldMapping struct {
	Name               *string `json:"name"`
	Type               *string `json:"type"`
	Analyzer           *string `json:"analyzer"`
	Store              *bool   `json:"store"`
	Index              *bool   `json:"index"`
	IncludeTermVectors *bool   `json:"include_term_vectors"`
	IncludeInAll       *bool   `json:"include_in_all"`
	DateFormat         *string `json:"date_format"`
}

// NewFieldMapping returns a FieldMapping with the
// specified behavior.
func NewFieldMapping(name, typ, analyzer string, store, index bool, includeTermVectors bool, includeInAll bool) *FieldMapping {
	return &FieldMapping{
		Name:               &name,
		Type:               &typ,
		Analyzer:           &analyzer,
		Store:              &store,
		Index:              &index,
		IncludeTermVectors: &includeTermVectors,
		IncludeInAll:       &includeInAll,
	}
}

func defaultNumericFieldMapping() *FieldMapping {
	typ := "number"
	store := true
	index := true
	return &FieldMapping{
		Type:  &typ,
		Store: &store,
		Index: &index,
	}
}

func defaultDateTimeFieldMapping() *FieldMapping {
	typ := "datetime"
	store := true
	index := true
	return &FieldMapping{
		Type:  &typ,
		Store: &store,
		Index: &index,
	}
}

func defaultTextFieldMapping() *FieldMapping {
	typ := "text"
	store := true
	index := true
	include := true
	return &FieldMapping{
		Type:               &typ,
		Store:              &store,
		Index:              &index,
		IncludeTermVectors: &include,
	}
}

// Options returns the indexing options for this field.
func (fm *FieldMapping) Options() document.IndexingOptions {
	var rv document.IndexingOptions
	if fm.Store != nil && *fm.Store {
		rv |= document.StoreField
	}
	if fm.Index != nil && *fm.Index {
		rv |= document.IndexField
	}
	if fm.IncludeTermVectors != nil && *fm.IncludeTermVectors {
		rv |= document.IncludeTermVectors
	}
	return rv
}

func (fm *FieldMapping) processString(propertyValueString string, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	options := fm.Options()
	if *fm.Type == "text" {
		analyzer := fm.analyzerForField(path, context)
		field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
		context.doc.AddField(field)

		if fm.IncludeInAll != nil && !*fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	} else if *fm.Type == "datetime" {
		dateTimeFormat := context.im.DefaultDateTimeParser
		if fm.DateFormat != nil {
			dateTimeFormat = *fm.DateFormat
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

func (fm *FieldMapping) processFloat64(propertyValFloat float64, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if *fm.Type == "number" {
		options := fm.Options()
		field := document.NewNumericFieldWithIndexingOptions(fieldName, indexes, propertyValFloat, options)
		context.doc.AddField(field)
	}
}

func (fm *FieldMapping) processTime(propertyValueTime time.Time, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if *fm.Type == "datetime" {
		options := fm.Options()
		field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, propertyValueTime, options)
		if err == nil {
			context.doc.AddField(field)
		} else {
			log.Printf("could not build date %v", err)
		}
	}
}

func (fm *FieldMapping) analyzerForField(path []string, context *walkContext) *analysis.Analyzer {
	analyzerName := context.dm.defaultAnalyzerName(path)
	if analyzerName == "" {
		analyzerName = context.im.DefaultAnalyzer
	}
	if fm.Analyzer != nil && *fm.Analyzer != "" {
		analyzerName = *fm.Analyzer
	}
	return context.im.analyzerNamed(analyzerName)
}

func getFieldName(pathString string, path []string, fieldMapping *FieldMapping) string {
	fieldName := pathString
	if fieldMapping.Name != nil && *fieldMapping.Name != "" {
		parentName := ""
		if len(path) > 1 {
			parentName = encodePath(path[:len(path)-1]) + pathSeparator
		}
		fieldName = parentName + *fieldMapping.Name
	}
	return fieldName
}
