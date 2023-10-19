//  Copyright (c) 2022 Couchbase, Inc.
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
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeSynonymField int

func init() {
	var f SynonymField
	reflectStaticSizeSynonymField = int(reflect.TypeOf(f).Size())
}

type SynonymField struct {
	name              string
	synDef            *index.SynonymDefinition
	analyzedSynDef    *index.SynonymDefinition
	analyzer          analysis.Analyzer
	options           index.FieldIndexingOptions
	numPlainTextBytes uint64
	length            int
	value             []byte
}

func (n *SynonymField) Size() int {
	return reflectStaticSizeSynonymField + size.SizeOfPtr +
		len(n.name)
}

func (n *SynonymField) Name() string {
	return n.name
}

func (n *SynonymField) NumPlainTextBytes() uint64 {
	return n.numPlainTextBytes
}

func (n *SynonymField) ArrayPositions() []uint64 {
	return nil
}

func (n *SynonymField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *SynonymField) Value() []byte {
	return n.value
}

func (n *SynonymField) EncodedFieldType() byte {
	return 'y'
}

func (n *SynonymField) AnalyzedLength() int {
	return n.length
}

func (n *SynonymField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

// This function is used to convert the token stream to a phrase by using the
// position attribute of the tokens for example if the token stream is
// "hello world" and the position of hello is 2 and world is 4 then the phrase
// will be ["hello","","world"]
// This would essentially maintain the number of stop words between two
// normal words and maintain the order of the words while also stripping
// stop words at the end and start of the phrase.
func tokenStreamToPhrase(tokens analysis.TokenStream) []string {
	firstPosition := int(^uint(0) >> 1)
	lastPosition := 0
	for _, token := range tokens {
		if token.Position < firstPosition {
			firstPosition = token.Position
		}
		if token.Position > lastPosition {
			lastPosition = token.Position
		}
	}
	phraseLen := lastPosition - firstPosition + 1
	rv := make([]string, phraseLen)
	if phraseLen > 0 {
		for _, token := range tokens {
			pos := token.Position - firstPosition
			rv[pos] = string(token.Term)
		}
	}
	return rv
}

// applies an analyzer to each string in a slice and returns the result slice.
// if the analyzer is nil, the original slice is returned.
func analyzeSlice(analyzer analysis.Analyzer, slice []string) []string {
	if analyzer == nil {
		return slice
	}
	loc := 0
	rv := make([]string, len(slice))
	for _, val := range slice {
		analyzedPhrase := tokenStreamToPhrase(analyzer.Analyze([]byte(val)))
		result := strings.Join(analyzedPhrase, " ")
		rv[loc] = result
		loc++
	}
	return rv
}

func (n *SynonymField) Analyze() {
	n.analyzedSynDef = &index.SynonymDefinition{
		MappingType: n.synDef.MappingType,
		Input:       analyzeSlice(n.analyzer, n.synDef.Input),
		Synonyms:    analyzeSlice(n.analyzer, n.synDef.Synonyms),
	}
}

func (n *SynonymField) AnalyzedSynonymDefinition() *index.SynonymDefinition {
	return n.analyzedSynDef
}

func NewSynonymField(metadataKey string, synDef *index.SynonymDefinition, analyzer analysis.Analyzer, collection string) *SynonymField {
	return &SynonymField{
		name:     metadataKey,
		synDef:   synDef,
		analyzer: analyzer,
		options:  index.IndexField,
	}
}
