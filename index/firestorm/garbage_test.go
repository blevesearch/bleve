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
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
)

func TestGarbageCleanup(t *testing.T) {
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

	rows := []struct {
		row     index.IndexRow
		garbage bool
	}{
		// needed for warmup to work
		{NewFieldRow(0, IDFieldName), false},
		// 3 documents, with 2 older versions
		{NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil), true},
		{NewTermFreqRow(0, nil, []byte("a"), 2, 0, 0.0, nil), false},
		{NewTermFreqRow(0, nil, []byte("b"), 3, 0, 0.0, nil), false},
		{NewTermFreqRow(0, nil, []byte("c"), 4, 0, 0.0, nil), true},
		{NewTermFreqRow(0, nil, []byte("c"), 5, 0, 0.0, nil), false},
		// additional records for these docs which should be removed
		{NewTermFreqRow(1, []byte("cat"), []byte("a"), 1, 3, 2.0, nil), true},
		{NewTermFreqRow(1, []byte("cat"), []byte("c"), 4, 1, 1.0, nil), true},
		{NewStoredRow([]byte("a"), 1, 1, nil, []byte("tcat")), true},
		{NewStoredRow([]byte("c"), 4, 1, nil, []byte("tcat")), true},
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.row.Key(), row.row.Value())
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

	// warmup ensures that deletedDocNums is seeded correctly
	err = f.(*Firestorm).warmup(kvreader)
	if err != nil {
		t.Fatal(err)
	}

	err = kvreader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now invoke garbage collector cleanup manually
	f.(*Firestorm).garbageCollector.cleanup()

	// assert that garbage rows are gone
	reader, err := f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows {
		v, err := reader.Get(row.row.Key())
		if err != nil {
			t.Fatal(err)
		}
		if v != nil && row.garbage {
			t.Errorf("garbage row not deleted, key: %s", row.row.Key())
		}
		if v == nil && !row.garbage {
			t.Errorf("non-garbage row deleted, key: %s", row.row.Key())
		}
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// assert that deletedDocsNumbers size is 0
	if f.(*Firestorm).compensator.GarbageCount() != 0 {
		t.Errorf("expected deletedDocsNumbers size to be 0, got %d", f.(*Firestorm).compensator.GarbageCount())
	}

}

func TestGarbageDontPanicOnEmptyDocs(t *testing.T) {
	idx, err := NewFirestorm("", nil, index.NewAnalysisQueue(1))
	if err != nil {
		t.Fatal(err)
	}
	f := idx.(*Firestorm)
	gc := NewGarbageCollector(f)
	gc.garbageSleep = 30 * time.Millisecond

	gc.Start()
	time.Sleep(40 * time.Millisecond)
	gc.Stop()
}
