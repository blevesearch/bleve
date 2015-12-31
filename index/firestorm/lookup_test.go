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

func TestLookups(t *testing.T) {
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
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.row.Key(), row.row.Value())
		err = kvwriter.ExecuteBatch(wb)
		if err != nil {
			t.Fatal(err)
		}
		// also see the compensator
		if tfr, ok := row.row.(*TermFreqRow); ok {
			f.(*Firestorm).compensator.Mutate(tfr.DocID(), tfr.DocNum())
			// expect this mutation to be in the in-flight list
			val := f.(*Firestorm).compensator.inFlight.Get(&InFlightItem{docID: tfr.DocID()})
			if val == nil {
				t.Errorf("expected key: % x to be in the inflight list", tfr.DocID())
			}
			f.(*Firestorm).lookuper.lookup(&InFlightItem{docID: tfr.DocID(), docNum: tfr.DocNum()})
			// now expect this mutation to NOT be in the in-flight list
			val = f.(*Firestorm).compensator.inFlight.Get(&InFlightItem{docID: tfr.DocID()})
			if val != nil {
				t.Errorf("expected key: % x to NOT be in the inflight list, got %v", tfr.DocID(), val)
			}
		}
	}

	// check that doc count is 3 at the end of this
	docCount, err := f.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != 3 {
		t.Errorf("expected doc count 3, got %d", docCount)
	}

}
