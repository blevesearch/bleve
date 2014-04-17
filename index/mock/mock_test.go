//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package mock

import (
	"reflect"
	"testing"

	_ "github.com/couchbaselabs/bleve/analysis/analyzers/standard_analyzer"
	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
)

func TestCRUD(t *testing.T) {
	i := NewMockIndex()

	// create doc, assert doc count goes up
	doc1 := document.NewDocument("1")
	doc1.AddField(document.NewTextField("name", []byte("marty")))
	i.Update(doc1)
	count := i.DocCount()
	if count != 1 {
		t.Errorf("expected document count to be 1, was: %d", count)
	}

	// add another doc, assert doc count goes up again
	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextField("name", []byte("bob")))
	i.Update(doc2)
	count = i.DocCount()
	if count != 2 {
		t.Errorf("expected document count to be 2, was: %d", count)
	}

	// search for doc with term that should exist
	expectedMatch := &index.TermFieldDoc{
		ID:   "1",
		Freq: 1,
		Norm: 1,
	}
	tfr, err := i.TermFieldReader([]byte("marty"), "name")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	match, err := tfr.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expectedMatch, match) {
		t.Errorf("got %v, expected %v", match, expectedMatch)
	}
	nomatch, err := tfr.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if nomatch != nil {
		t.Errorf("expected nil after last match")
	}

	// update doc, assert doc count doesn't go up
	doc1 = document.NewDocument("1")
	doc1.AddField(document.NewTextField("name", []byte("salad")))
	doc1.AddField(document.NewTextFieldWithIndexingOptions("desc", []byte("eat more rice"), document.INDEX_FIELD|document.INCLUDE_TERM_VECTORS))
	i.Update(doc1)
	count = i.DocCount()
	if count != 2 {
		t.Errorf("expected document count to be 2, was: %d", count)
	}

	// perform the original search again, should NOT find anything this time
	tfr, err = i.TermFieldReader([]byte("marty"), "name")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nomatch, err = tfr.Next()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if nomatch != nil {
		t.Errorf("expected no matches, found one")
		t.Logf("%v", i)
	}

	// delete a doc, ensure the count is 1
	err = i.Delete("2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	count = i.DocCount()
	if count != 1 {
		t.Errorf("expected document count to be 1, was: %d", count)
	}

	expectedMatch = &index.TermFieldDoc{
		ID:   "1",
		Freq: 1,
		Norm: 0.5773502691896258,
		Vectors: []*index.TermFieldVector{
			&index.TermFieldVector{
				Field: "desc",
				Pos:   3,
				Start: 9,
				End:   13,
			},
		},
	}
	tfr, err = i.TermFieldReader([]byte("rice"), "desc")
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
}
