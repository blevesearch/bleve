//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package document

import (
	"log"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/analysis/analyzers/standard_analyzer"
)

var standardAnalyzer *analysis.Analyzer

func init() {
	var err error
	standardAnalyzer, err = standard_analyzer.NewStandardAnalyzer()
	if err != nil {
		log.Fatal(err)
	}
}

const DEFAULT_TEXT_INDEXING_OPTIONS = INDEX_FIELD

func NewTextField(name string, value []byte) *Field {
	return NewTextFieldWithIndexingOptions(name, value, DEFAULT_TEXT_INDEXING_OPTIONS)
}

func NewTextFieldWithIndexingOptions(name string, value []byte, indexingOptions int) *Field {
	return &Field{
		Name:            name,
		IndexingOptions: indexingOptions,
		Analyzer:        standardAnalyzer,
		Value:           value,
	}
}
