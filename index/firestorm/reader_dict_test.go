//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/tokenizers/regexp_tokenizer"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
)

var testAnalyzer = &analysis.Analyzer{
	Tokenizer: regexp_tokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
}

func TestDictionaryReader(t *testing.T) {
	aq := index.NewAnalysisQueue(1)
	f, err := NewFirestorm(gtreap.Name, nil, aq)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Open()
	if err != nil {
		t.Fatal(err)
	}

	kvwriter, err := f.(*Firestorm).store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	rows := []index.IndexRow{
		NewFieldRow(0, IDFieldName),
		NewFieldRow(1, "name"),
		NewFieldRow(2, "desc"),
		NewFieldRow(3, "prefix"),
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.Key(), row.Value())
		err = kvwriter.ExecuteBatch(wb)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = kvwriter.Close()
	if err != nil {
		t.Fatal(err)
	}

	kvreader, err := f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	err = f.(*Firestorm).warmup(kvreader)
	if err != nil {
		t.Fatal(err)
	}

	err = kvreader.Close()
	if err != nil {
		t.Fatal(err)
	}

	kvwriter, err = f.(*Firestorm).store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	rows = []index.IndexRow{

		// dictionary entries
		NewDictionaryRow(1, []byte("test"), 4),

		NewDictionaryRow(2, []byte("eat"), 1),
		NewDictionaryRow(2, []byte("more"), 1),
		NewDictionaryRow(2, []byte("rice"), 1),

		NewDictionaryRow(3, []byte("bob"), 1),
		NewDictionaryRow(3, []byte("cat"), 1),
		NewDictionaryRow(3, []byte("cats"), 1),
		NewDictionaryRow(3, []byte("catting"), 1),
		NewDictionaryRow(3, []byte("dog"), 1),
		NewDictionaryRow(3, []byte("doggy"), 1),
		NewDictionaryRow(3, []byte("zoo"), 1),
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.Key(), row.Value())
		err = kvwriter.ExecuteBatch(wb)
		if err != nil {
			t.Fatal(err)
		}
	}

	// now try it
	r, err := f.Reader()
	if err != nil {
		t.Fatal(err)
	}

	dict, err := r.FieldDict("name")
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}

	termCount := 0
	curr, err := dict.Next()
	for err == nil && curr != nil {
		termCount++
		if curr.Term != "test" {
			t.Errorf("expected term to be 'test', got '%s'", curr.Term)
		}
		curr, err = dict.Next()
	}
	if termCount != 1 {
		t.Errorf("expected 1 term for this field, got %d", termCount)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	dict, err = r.FieldDict("desc")
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}

	termCount = 0
	terms := make([]string, 0)
	curr, err = dict.Next()
	for err == nil && curr != nil {
		termCount++
		terms = append(terms, curr.Term)
		curr, err = dict.Next()
	}
	if termCount != 3 {
		t.Errorf("expected 3 term for this field, got %d", termCount)
	}
	expectedTerms := []string{"eat", "more", "rice"}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected %#v, got %#v", expectedTerms, terms)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	// test start and end range
	dict, err = r.FieldDictRange("desc", []byte("fun"), []byte("nice"))
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}

	termCount = 0
	terms = make([]string, 0)
	curr, err = dict.Next()
	for err == nil && curr != nil {
		termCount++
		terms = append(terms, curr.Term)
		curr, err = dict.Next()
	}
	if termCount != 1 {
		t.Errorf("expected 1 term for this field, got %d", termCount)
	}
	expectedTerms = []string{"more"}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected %#v, got %#v", expectedTerms, terms)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	// test use case for prefix
	dict, err = r.FieldDictPrefix("prefix", []byte("cat"))
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}

	termCount = 0
	terms = make([]string, 0)
	curr, err = dict.Next()
	for err == nil && curr != nil {
		termCount++
		terms = append(terms, curr.Term)
		curr, err = dict.Next()
	}
	if termCount != 3 {
		t.Errorf("expected 3 term for this field, got %d", termCount)
	}
	expectedTerms = []string{"cat", "cats", "catting"}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected %#v, got %#v", expectedTerms, terms)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
