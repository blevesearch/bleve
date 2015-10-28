package upside_down

import (
	"testing"

	"github.com/blevesearch/bleve/analysis/analyzers/standard_analyzer"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/null"
	"github.com/blevesearch/bleve/registry"
)

func BenchmarkAnalyze(b *testing.B) {

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(standard_analyzer.Name)
	if err != nil {
		b.Fatal(err)
	}

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewUpsideDownCouch(null.Name, nil, analysisQueue)
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
