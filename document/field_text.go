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

type TextField struct {
	name     string
	options  IndexingOptions
	analyzer *analysis.Analyzer
	value    []byte
}

func (t *TextField) Name() string {
	return t.name
}

func (t *TextField) Options() IndexingOptions {
	return t.options
}

func (t *TextField) Analyze() (int, analysis.TokenFrequencies) {
	tokens := t.analyzer.Analyze(t.Value())
	fieldLength := len(tokens) // number of tokens in this doc field
	tokenFreqs := analysis.TokenFrequency(tokens)
	return fieldLength, tokenFreqs
}

func (t *TextField) Value() []byte {
	return t.value
}

func NewTextField(name string, value []byte) *TextField {
	return NewTextFieldWithIndexingOptions(name, value, DEFAULT_TEXT_INDEXING_OPTIONS)
}

func NewTextFieldWithIndexingOptions(name string, value []byte, options IndexingOptions) *TextField {
	return &TextField{
		name:     name,
		options:  options,
		analyzer: standardAnalyzer,
		value:    value,
	}
}

func NewTextFieldCustom(name string, value []byte, options IndexingOptions, analyzer *analysis.Analyzer) *TextField {
	return &TextField{
		name:     name,
		options:  options,
		analyzer: analyzer,
		value:    value,
	}
}
