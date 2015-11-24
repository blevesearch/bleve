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
	"math/rand"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
)

func TestDocIDReaderSomeGarbage(t *testing.T) {
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
		NewFieldRow(1, "desc"),
		NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("b"), 2, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("c"), 3, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("d"), 4, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("a"), 5, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("b"), 6, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("e"), 7, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("g"), 8, 0, 0.0, nil),
		// first version of all docs have cat
		NewTermFreqRow(1, []byte("cat"), []byte("a"), 1, 1, 1.0, nil),
		NewTermFreqRow(1, []byte("cat"), []byte("b"), 2, 1, 1.0, nil),
		NewTermFreqRow(1, []byte("cat"), []byte("c"), 3, 1, 1.0, nil),
		NewTermFreqRow(1, []byte("cat"), []byte("d"), 4, 1, 1.0, nil),
		NewTermFreqRow(1, []byte("cat"), []byte("e"), 7, 1, 1.0, nil),
		NewTermFreqRow(1, []byte("cat"), []byte("g"), 8, 1, 1.0, nil),
		// updated version of a still has cat
		NewTermFreqRow(1, []byte("cat"), []byte("a"), 5, 1, 1.0, nil),
		// updated version of b does NOT have cat
		// c has delete in-flight
		// d has delete not-yet-garbage-collected
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.Key(), row.Value())
		err = kvwriter.ExecuteBatch(wb)
		if err != nil {
			t.Fatal(err)
		}
	}

	f.(*Firestorm).compensator.inFlight = f.(*Firestorm).compensator.inFlight.Upsert(&InFlightItem{docID: []byte("c"), docNum: 0}, rand.Int())
	f.(*Firestorm).compensator.deletedDocNumbers.Set(4)

	err = kvwriter.Close()
	if err != nil {
		t.Fatal(err)
	}

	kvreader, err := f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	// warmup to load field cache and set maxRead correctly
	err = f.(*Firestorm).warmup(kvreader)
	if err != nil {
		t.Fatal(err)
	}

	err = kvreader.Close()
	if err != nil {
		t.Fatal(err)
	}

	r, err := f.Reader()
	if err != nil {
		t.Fatal(err)
	}

	dr, err := r.DocIDReader("", "")
	if err != nil {
		t.Fatal(err)
	}

	expectedDocIds := []string{"a", "b", "e", "g"}
	foundDocIds := make([]string, 0)
	next, err := dr.Next()
	for next != "" && err == nil {
		foundDocIds = append(foundDocIds, next)
		next, err = dr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedDocIds, foundDocIds) {
		t.Errorf("expected: %v, got %v", expectedDocIds, foundDocIds)
	}

	err = dr.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now test with some doc id ranges
	dr, err = r.DocIDReader("b", "f")
	if err != nil {
		t.Fatal(err)
	}

	expectedDocIds = []string{"b", "e"}
	foundDocIds = make([]string, 0)
	next, err = dr.Next()
	for next != "" && err == nil {
		foundDocIds = append(foundDocIds, next)
		next, err = dr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedDocIds, foundDocIds) {
		t.Errorf("expected: %v, got %v", expectedDocIds, foundDocIds)
	}

	err = dr.Close()
	if err != nil {
		t.Fatal(err)
	}

	//now try again and Advance to skip over "e"
	dr, err = r.DocIDReader("b", "")
	if err != nil {
		t.Fatal(err)
	}

	expectedDocIds = []string{"b", "g"}

	foundDocIds = make([]string, 0)
	next, err = dr.Next()
	if err != nil {
		t.Fatal(err)
	} else {
		foundDocIds = append(foundDocIds, next)
	}
	next, err = dr.Advance("f")
	if err != nil {
		t.Fatal(err)
	} else {
		foundDocIds = append(foundDocIds, next)
	}

	if !reflect.DeepEqual(expectedDocIds, foundDocIds) {
		t.Errorf("expected: %v, got %v", expectedDocIds, foundDocIds)
	}

	err = dr.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
