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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/boltdb"
)

func TestIndexReader(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{}, []byte("test test test"), testAnalyzer))
	doc.AddField(document.NewTextFieldCustom("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors, testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	// first look for a term that doesnt exist
	reader, err := idx.TermFieldReader([]byte("nope"), "name")
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count := reader.Count()
	if count != 0 {
		t.Errorf("Expected doc count to be: %d got: %d", 0, count)
	}
	reader.Close()

	reader, err = idx.TermFieldReader([]byte("test"), "name")
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	expectedCount = 2
	count = reader.Count()
	if count != expectedCount {
		t.Errorf("Exptected doc count to be: %d got: %d", expectedCount, count)
	}

	var match *index.TermFieldDoc
	var actualCount uint64
	match, err = reader.Next()
	for err == nil && match != nil {
		match, err = reader.Next()
		if err != nil {
			t.Errorf("unexpected error reading next")
		}
		actualCount += 1
	}
	if actualCount != count {
		t.Errorf("count was 2, but only saw %d", actualCount)
	}

	expectedMatch := &index.TermFieldDoc{
		ID:   "2",
		Freq: 1,
		Norm: 0.5773502588272095,
		Vectors: []*index.TermFieldVector{
			&index.TermFieldVector{
				Field: "desc",
				Pos:   3,
				Start: 9,
				End:   13,
			},
		},
	}
	tfr, err := idx.TermFieldReader([]byte("rice"), "desc")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	match, err = tfr.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expectedMatch, match) {
		t.Errorf("got %#v, expected %#v", match, expectedMatch)
	}
	reader.Close()

	// now test usage of advance
	reader, err = idx.TermFieldReader([]byte("test"), "name")
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	match, err = reader.Advance("2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match == nil {
		t.Fatalf("Expected match, got nil")
	}
	if match.ID != "2" {
		t.Errorf("Expected ID '2', got '%s'", match.ID)
	}
	match, err = reader.Advance("3")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}
	reader.Close()

	// now test creating a reader for a field that doesn't exist
	reader, err = idx.TermFieldReader([]byte("water"), "doesnotexist")
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count = reader.Count()
	if count != 0 {
		t.Errorf("expected count 0 for reader of non-existant field")
	}
	match, err = reader.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}
	match, err = reader.Advance("anywhere")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}

}

func TestIndexDocIdReader(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test test test")))
	doc.AddField(document.NewTextFieldWithIndexingOptions("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	// first get all doc ids
	reader, err := idx.DocIdReader("", "")
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer reader.Close()

	id, err := reader.Next()
	count := uint64(0)
	for id != "" {
		count++
		id, err = reader.Next()
	}
	if count != expectedCount {
		t.Errorf("expected %d, got %d", expectedCount, count)
	}

	// try it again, but jump to the second doc this time
	reader, err = idx.DocIdReader("", "")
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer reader.Close()

	id, err = reader.Advance("2")
	if err != nil {
		t.Error(err)
	}
	if id != "2" {
		t.Errorf("expected to find id '2', got '%s'", id)
	}

	id, err = reader.Advance("3")
	if err != nil {
		t.Error(err)
	}
	if id != "" {
		t.Errorf("expected to find id '', got '%s'", id)
	}
}
