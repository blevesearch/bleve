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
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/inmem"
)

func TestAnalysis(t *testing.T) {

	kv, err := inmem.New()
	if err != nil {
		t.Fatal(err)
	}
	aq := index.NewAnalysisQueue(1)
	f := NewFirestorm(kv, aq)

	rows := []index.IndexRow{
		NewFieldRow(0, IDFieldName),
	}

	kvwriter, err := f.store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows {
		err := kvwriter.Set(row.Key(), row.Value())
		if err != nil {
			t.Fatal(err)
		}
	}

	// warmup to load field cache and set maxRead correctly
	f.warmup(kvwriter)

	tests := []struct {
		d *document.Document
		r *index.AnalysisResult
	}{
		{
			d: document.NewDocument("a").
				AddField(
				document.NewTextFieldWithIndexingOptions("name", nil, []byte("test"), document.IndexField|document.StoreField|document.IncludeTermVectors)),
			r: &index.AnalysisResult{
				DocID: "a",
				Rows: []index.IndexRow{
					NewFieldRow(1, "name"),
					NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil),
					NewTermFreqRow(1, []byte("test"), []byte("a"), 1, 1, 1.0, []*TermVector{NewTermVector(1, 1, 0, 4, nil)}),
					NewStoredRow([]byte("a"), 1, 1, nil, []byte("ttest")),
				},
			},
		},
	}

	for _, test := range tests {
		test.d.Number = 1
		actual := f.Analyze(test.d)
		if !reflect.DeepEqual(actual, test.r) {
			t.Errorf("expected: %v got %v", test.r, actual)
		}
	}

	err = kvwriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
