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
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
)

func TestBootstrap(t *testing.T) {
	aq := index.NewAnalysisQueue(1)
	f, err := NewFirestorm(gtreap.Name, nil, aq)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Open() // open calls bootstrap
	if err != nil {
		t.Fatal(err)
	}

	// assert that version is set
	reader, err := f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}
	val, err := reader.Get(VersionKey)
	if err != nil {
		t.Fatal(err)
	}
	verRow, err := NewVersionRowV(val)
	if err != nil {
		t.Fatal(err)
	}
	if verRow.Version() != Version {
		t.Errorf("expected version %d, got %d", Version, verRow.Version())
	}

	// assert that field cache has _id
	id, existed := f.(*Firestorm).fieldCache.FieldNamed(IDFieldName, false)
	if !existed {
		t.Errorf("expect '%s' in field cache", IDFieldName)
	}
	if id != 0 {
		t.Errorf("expected '%s' to have index 0, got %d", IDFieldName, id)
	}

	// assert that field is recorded in kv store
	fRowExpected := NewFieldRow(id, IDFieldName)
	fRowKey := fRowExpected.Key()
	val, err = reader.Get(fRowKey)
	if err != nil {
		t.Fatal(err)
	}
	fRowActual, err := NewFieldRowKV(fRowKey, val)
	if err != nil {
		t.Fatal(err)
	}
	if fRowExpected.Name() != fRowActual.Name() {
		t.Errorf("expected name '%s' got '%s'", fRowExpected.Name(), fRowActual.Name())
	}

	// assert that highDocNumber is 0
	if f.(*Firestorm).highDocNumber != 0 {
		t.Errorf("expected highDocNumber to be 0, got %d", f.(*Firestorm).highDocNumber)
	}

}

func TestWarmupNoGarbage(t *testing.T) {
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
		NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("b"), 2, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("c"), 3, 0, 0.0, nil),
	}
	expectedCount := uint64(3)
	expectedGarbage := uint64(0)

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

	// assert that doc count is correct
	count, err := f.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != expectedCount {
		t.Errorf("expected doc count %d, got %d", expectedCount, count)
	}

	// assert that deleted doc numbers size is 0
	if f.(*Firestorm).compensator.GarbageCount() != expectedGarbage {
		t.Errorf("expected 0 deleted doc numbers, got %d", f.(*Firestorm).compensator.GarbageCount())
	}

	// assert that highDocNumber is 3
	if f.(*Firestorm).highDocNumber != 3 {
		t.Errorf("expected highDocNumber to be 3, got %d", f.(*Firestorm).highDocNumber)
	}
}

func TestWarmupSomeGarbage(t *testing.T) {
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
		NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("a"), 2, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("b"), 3, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("c"), 4, 0, 0.0, nil),
		NewTermFreqRow(0, nil, []byte("c"), 5, 0, 0.0, nil),
	}
	expectedCount := uint64(3)
	expectedGarbage := uint64(2)

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

	// assert that doc count is correct
	count, err := f.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != expectedCount {
		t.Errorf("expected doc count %d, got %d", expectedCount, count)
	}

	// assert that deleted doc numbers size is 0
	if f.(*Firestorm).compensator.GarbageCount() != expectedGarbage {
		t.Errorf("expected %d deleted doc numbers, got %d", expectedGarbage, f.(*Firestorm).compensator.GarbageCount())
	}

	// assert that doc numbers 1 and 4 are on the deleted list
	if !f.(*Firestorm).compensator.deletedDocNumbers.Test(1) {
		t.Errorf("expected doc number 1 to be deleted")
	}
	if !f.(*Firestorm).compensator.deletedDocNumbers.Test(4) {
		t.Errorf("expected doc number 4 to be deleted")
	}

	// assert that highDocNumber is 5
	if f.(*Firestorm).highDocNumber != 5 {
		t.Errorf("expected highDocNumber to be 5, got %d", f.(*Firestorm).highDocNumber)
	}
}
