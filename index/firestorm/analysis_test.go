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
	"github.com/blevesearch/bleve/index/store/inmem"
	"github.com/blevesearch/bleve/index/store/null"
	"github.com/blevesearch/bleve/registry"
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

func BenchmarkAnalyze(b *testing.B) {

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(standard_analyzer.Name)
	if err != nil {
		b.Fatal(err)
	}

	s, err := null.New()
	if err != nil {
		b.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx := NewFirestorm(s, analysisQueue)

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
