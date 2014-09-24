//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"os"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index/store/boltdb"
)

func TestIndexFieldReader(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	analysisQueue := NewAnalysisQueue(1)
	idx := NewUpsideDownCouch(store, analysisQueue)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{}, []byte("test test test"), testAnalyzer))
	doc.AddField(document.NewTextFieldCustom("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors, testAnalyzer))
	doc.AddField(document.NewTextFieldCustom("prefix", []uint64{}, []byte("bob cat cats catting dog doggy zoo"), document.IndexField|document.IncludeTermVectors, testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	indexReader := idx.Reader()
	defer indexReader.Close()
	reader, err := indexReader.FieldReader("name", nil, nil)
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}
	defer reader.Close()

	termCount := 0
	curr, err := reader.Next()
	for err == nil && curr != nil {
		termCount++
		if curr.Term != "test" {
			t.Errorf("expected term to be 'test', got '%s'", curr.Term)
		}
		curr, err = reader.Next()
	}
	if termCount != 1 {
		t.Errorf("expected 1 term for this field, got %d", termCount)
	}

	reader, err = indexReader.FieldReader("desc", nil, nil)
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}
	defer reader.Close()

	termCount = 0
	terms := make([]string, 0)
	curr, err = reader.Next()
	for err == nil && curr != nil {
		termCount++
		terms = append(terms, curr.Term)
		curr, err = reader.Next()
	}
	if termCount != 3 {
		t.Errorf("expected 3 term for this field, got %d", termCount)
	}
	expectedTerms := []string{"eat", "more", "rice"}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected %#v, got %#v", expectedTerms, terms)
	}

	// test use case for prefix
	reader, err = indexReader.FieldReader("prefix", []byte("cat"), []byte("cat"))
	if err != nil {
		t.Errorf("error creating reader: %v", err)
	}
	defer reader.Close()

	termCount = 0
	terms = make([]string, 0)
	curr, err = reader.Next()
	for err == nil && curr != nil {
		termCount++
		terms = append(terms, curr.Term)
		curr, err = reader.Next()
	}
	if termCount != 3 {
		t.Errorf("expected 3 term for this field, got %d", termCount)
	}
	expectedTerms = []string{"cats", "catting", "cat"}
	if !reflect.DeepEqual(expectedTerms, terms) {
		t.Errorf("expected %#v, got %#v", expectedTerms, terms)
	}
}
