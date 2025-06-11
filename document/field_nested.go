//  Copyright (c) 2025 Couchbase, Inc.
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

package document

import (
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeNestedField int

func init() {
	var f NestedField
	reflectStaticSizeNestedField = int(reflect.TypeOf(f).Size())
}

const DefaultNestedIndexingOptions = index.IndexField

type NestedField struct {
	name              string
	options           index.FieldIndexingOptions
	numPlainTextBytes uint64

	nestedDocuments []index.Document

	docAnalyzer index.DocumentAnalyzer
}

func (s *NestedField) Size() int {
	return reflectStaticSizeNestedField + size.SizeOfPtr +
		len(s.name)
}

func (s *NestedField) Name() string {
	return s.name
}

func (s *NestedField) ArrayPositions() []uint64 {
	return nil
}

func (s *NestedField) Options() index.FieldIndexingOptions {
	return s.options
}

func (s *NestedField) NumPlainTextBytes() uint64 {
	return s.numPlainTextBytes
}

func (s *NestedField) AnalyzedLength() int {
	return 0
}

func (s *NestedField) EncodedFieldType() byte {
	return 'e'
}

func (s *NestedField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

func (s *NestedField) Analyze() {
	for _, doc := range s.nestedDocuments {
		s.docAnalyzer.Analyze(doc)
	}
}

func (s *NestedField) Value() []byte {
	return nil
}

func NewNestedField(name string, nestedDocuments []index.Document, docAnalyzer index.DocumentAnalyzer) *NestedField {
	return &NestedField{
		name:            name,
		options:         DefaultNestedIndexingOptions,
		nestedDocuments: nestedDocuments,
		docAnalyzer:     docAnalyzer,
	}
}
