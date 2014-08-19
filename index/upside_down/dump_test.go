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
	"github.com/couchbaselabs/bleve/index/store/leveldb"
	"os"
	"testing"
	"time"

	"github.com/couchbaselabs/bleve/document"
)

func TestDump(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := leveldb.Open("test", true)
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test2"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	fieldsCount := 0
	fieldsRows := idx.DumpFields()
	for _ = range fieldsRows {
		fieldsCount++
	}
	if fieldsCount != 3 {
		t.Errorf("expected 3 fields, got %d", fieldsCount)
	}

	// 1 text term
	// 16 numeric terms
	// 16 date terms
	// 3 stored fields
	expectedDocRowCount := int(1 + (2 * (64 / document.DEFAULT_PRECISION_STEP)) + 3)
	docRowCount := 0
	docRows := idx.DumpDoc("1")
	for _ = range docRows {
		docRowCount++
	}
	if docRowCount != expectedDocRowCount {
		t.Errorf("expected %d rows for document, got %d", expectedDocRowCount, docRowCount)
	}

	docRowCount = 0
	docRows = idx.DumpDoc("2")
	for _ = range docRows {
		docRowCount++
	}
	if docRowCount != expectedDocRowCount {
		t.Errorf("expected %d rows for document, got %d", expectedDocRowCount, docRowCount)
	}

	// 1 version
	// fieldsCount field rows
	// 2 docs * expectedDocRowCount
	// 2 back index rows
	// 2 text term row count (2 different text terms)
	// 16 numeric term row counts (shared for both docs, same numeric value)
	// 16 date term row counts (shared for both docs, same date value)
	expectedAllRowCount := int(1 + fieldsCount + (2 * expectedDocRowCount) + 2 + 2 + int((2 * (64 / document.DEFAULT_PRECISION_STEP))))
	allRowCount := 0
	allRows := idx.DumpAll()
	for _ = range allRows {
		allRowCount++
	}
	if allRowCount != expectedAllRowCount {
		t.Errorf("expected %d rows for all, got %d", expectedAllRowCount, allRowCount)
	}
}
