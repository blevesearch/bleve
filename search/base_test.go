//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"math"
	"regexp"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/analysis/tokenizers/regexp_tokenizer"
	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
	"github.com/couchbaselabs/bleve/index/store/inmem"
	"github.com/couchbaselabs/bleve/index/upside_down"
)

var twoDocIndex index.Index //= upside_down.NewUpsideDownCouch(inmem.MustOpen())

func init() {
	inMemStore, _ := inmem.Open()
	twoDocIndex = upside_down.NewUpsideDownCouch(inMemStore)
	for _, doc := range twoDocIndexDocs {
		twoDocIndex.Update(doc)
	}
}

// create a simpler analyzer which will support these tests
var testAnalyzer = &analysis.Analyzer{
	Tokenizer: regexp_tokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
}

// sets up some mock data used in many tests in this package
var twoDocIndexDescIndexingOptions = document.DEFAULT_TEXT_INDEXING_OPTIONS | document.INCLUDE_TERM_VECTORS

var twoDocIndexDocs = []*document.Document{
	// must have 4/4 beer
	document.NewDocument("1").
		AddField(document.NewTextField("name", []byte("marty"))).
		AddField(document.NewTextFieldCustom("desc", []byte("beer beer beer beer"), twoDocIndexDescIndexingOptions, testAnalyzer)).
		AddField(document.NewTextFieldWithAnalyzer("street", []byte("couchbase way"), testAnalyzer)),
	// must have 1/4 beer
	document.NewDocument("2").
		AddField(document.NewTextField("name", []byte("steve"))).
		AddField(document.NewTextFieldCustom("desc", []byte("angst beer couch database"), twoDocIndexDescIndexingOptions, testAnalyzer)).
		AddField(document.NewTextFieldWithAnalyzer("street", []byte("couchbase way"), testAnalyzer)).
		AddField(document.NewTextFieldWithAnalyzer("title", []byte("mister"), testAnalyzer)),
	// must have 1/4 beer
	document.NewDocument("3").
		AddField(document.NewTextField("name", []byte("dustin"))).
		AddField(document.NewTextFieldCustom("desc", []byte("apple beer column dank"), twoDocIndexDescIndexingOptions, testAnalyzer)).
		AddField(document.NewTextFieldWithAnalyzer("title", []byte("mister"), testAnalyzer)),
	// must have 65/65 beer
	document.NewDocument("4").
		AddField(document.NewTextField("name", []byte("ravi"))).
		AddField(document.NewTextFieldCustom("desc", []byte("beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer"), twoDocIndexDescIndexingOptions, testAnalyzer)),
	// must have 0/x beer
	document.NewDocument("5").
		AddField(document.NewTextField("name", []byte("bobert"))).
		AddField(document.NewTextFieldCustom("desc", []byte("water"), twoDocIndexDescIndexingOptions, testAnalyzer)).
		AddField(document.NewTextFieldWithAnalyzer("title", []byte("mister"), testAnalyzer)),
}

func scoresCloseEnough(a, b float64) bool {
	return math.Abs(a-b) < 0.001
}
