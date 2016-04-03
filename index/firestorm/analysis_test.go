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

	"github.com/blevesearch/bleve/analysis/analyzers/standard_analyzer"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/store/null"
	"github.com/blevesearch/bleve/registry"
)

func TestAnalysis(t *testing.T) {

	aq := index.NewAnalysisQueue(1)
	f, err := NewFirestorm(gtreap.Name, nil, aq)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Open()
	if err != nil {
		t.Fatal(err)
	}

	rows := []index.IndexRow{
		NewFieldRow(0, IDFieldName),
	}

	kvwriter, err := f.(*Firestorm).store.Writer()
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows {
		wb := kvwriter.NewBatch()
		wb.Set(row.Key(), row.Value())
		err := kvwriter.ExecuteBatch(wb)
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
	// warmup to load field cache and set maxRead correctly
	err = f.(*Firestorm).warmup(kvreader)
	if err != nil {
		t.Fatal(err)
	}

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
					NewTermFreqRow(0, nil, []byte("a"), 1, 0, 0.0, nil),
					NewFieldRow(1, "name"),
					NewStoredRow([]byte("a"), 1, 1, nil, []byte("ttest")),
					NewTermFreqRow(1, []byte("test"), []byte("a"), 1, 1, 1.0, []*TermVector{NewTermVector(1, 1, 0, 4, nil)}),
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

	err = kvreader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnalysisBug328(t *testing.T) {
	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(standard_analyzer.Name)
	if err != nil {
		t.Fatal(err)
	}

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewFirestorm(gtreap.Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	d := document.NewDocument("1")
	f := document.NewTextFieldCustom("title", nil, []byte("bleve"), document.IndexField|document.IncludeTermVectors, analyzer)
	d.AddField(f)
	f = document.NewTextFieldCustom("body", nil, []byte("bleve"), document.IndexField|document.IncludeTermVectors, analyzer)
	d.AddField(f)
	cf := document.NewCompositeFieldWithIndexingOptions("_all", true, []string{}, []string{}, document.IndexField|document.IncludeTermVectors)
	d.AddField(cf)

	rv := idx.Analyze(d)
	fieldIndexes := make(map[uint16]string)
	for _, row := range rv.Rows {
		if row, ok := row.(*FieldRow); ok {
			fieldIndexes[row.index] = row.Name()
		}
		if row, ok := row.(*TermFreqRow); ok && string(row.term) == "bleve" {
			for _, vec := range row.Vectors() {
				if vec.GetField() != uint32(row.field) {
					if fieldIndexes[row.field] != "_all" {
						t.Errorf("row named %s field %d - vector field %d", fieldIndexes[row.field], row.field, vec.GetField())
					}
				}
			}
		}
	}
}

func BenchmarkAnalyze(b *testing.B) {

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(standard_analyzer.Name)
	if err != nil {
		b.Fatal(err)
	}

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewFirestorm(null.Name, nil, analysisQueue)
	if err != nil {
		b.Fatal(err)
	}

	d := document.NewDocument("1")
	f := document.NewTextFieldWithAnalyzer("desc", nil, bleveWikiArticle1K, analyzer)
	d.AddField(f)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rv := idx.Analyze(d)
		if len(rv.Rows) < 92 || len(rv.Rows) > 93 {
			b.Fatalf("expected 512-13 rows, got %d", len(rv.Rows))
		}
	}
}

var bleveWikiArticle1K = []byte(`Boiling liquid expanding vapor explosion
From Wikipedia, the free encyclopedia
See also: Boiler explosion and Steam explosion

Flames subsequent to a flammable liquid BLEVE from a tanker. BLEVEs do not necessarily involve fire.

This article's tone or style may not reflect the encyclopedic tone used on Wikipedia. See Wikipedia's guide to writing better articles for suggestions. (July 2013)
A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.[1]
Contents  [hide]
1 Mechanism
1.1 Water example
1.2 BLEVEs without chemical reactions
2 Fires
3 Incidents
4 Safety measures
5 See also
6 References
7 External links
Mechanism[edit]

This section needs additional citations for verification. Please help improve this article by adding citations to reliable sources. Unsourced material may be challenged and removed. (July 2013)
There are three characteristics of liquids which are relevant to the discussion of a BLEVE:`)
