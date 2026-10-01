//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bleve

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// buildAllocBenchIndex builds the corpus of the allocation benchmarks: many
// documents spread over several segments, as the latency benchmark's.
func buildAllocBenchIndex(b *testing.B, numDocs, numSegments int) (Index, func()) {
	return buildAllocBenchIndexModel(b, numDocs, numSegments, "")
}

// buildAllocBenchIndexModel is buildAllocBenchIndex with a scoring model: "bm25"
// or, by default, tf-idf.
func buildAllocBenchIndexModel(b *testing.B, numDocs, numSegments int, model string) (Index, func()) {
	b.Helper()
	dir := createTmpIndexPath(b)
	im := NewIndexMapping()
	if model == "bm25" {
		im.ScoringModel = index.BM25Scoring
	}
	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, map[string]interface{}{
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	})
	if err != nil {
		b.Fatal(err)
	}

	rnd := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rnd, 1.2, 4, 4999)
	markers := []struct {
		term string
		pct  float64
	}{{"mk100", 100}, {"mk50", 50}, {"mk10", 10}, {"mk1", 1}, {"mk01", 0.1}}

	per := numDocs / numSegments
	for bt := 0; bt < numSegments; bt++ {
		batch := idx.NewBatch()
		for i := bt * per; i < (bt+1)*per; i++ {
			var sb strings.Builder
			for k := 3 + rnd.Intn(38); k > 0; k-- {
				sb.WriteString("w")
				sb.WriteString(strconv.FormatUint(zipf.Uint64(), 10))
				sb.WriteByte(' ')
			}
			for _, m := range markers {
				if rnd.Float64()*100 < m.pct {
					for r := 1 + rnd.Intn(3); r > 0; r-- {
						sb.WriteString(m.term)
						sb.WriteByte(' ')
					}
				}
			}
			if err := batch.Index(strconv.Itoa(i), map[string]interface{}{"body": sb.String()}); err != nil {
				b.Fatal(err)
			}
		}
		persisted := make(chan error, 1)
		batch.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(batch); err != nil {
			b.Fatal(err)
		}
		if err := <-persisted; err != nil {
			b.Fatal(err)
		}
	}
	return idx, func() { _ = idx.Close(); cleanupTmpIndexPath(b, dir) }
}

// BenchmarkSearchAllocs measures what a search costs in time and allocations,
// per shape, on the per segment path and on the regular one, to tell where the
// garbage comes from:
//
//	go test -run xxx -bench SearchAllocs -benchmem [-memprofile mem.out -memprofilerate 1]
func BenchmarkSearchAllocs(b *testing.B) {
	idx, cleanup := buildAllocBenchIndex(b, 200000, 8)
	defer cleanup()

	term := func(t string) query.Query {
		q := query.NewTermQuery(t)
		q.SetField("body")
		return q
	}
	shapes := []struct {
		name string
		q    func() query.Query
		size int
	}{
		{"term", func() query.Query { return term("mk10") }, 10},
		{"and", func() query.Query { return query.NewConjunctionQuery([]query.Query{term("mk50"), term("mk10")}) }, 10},
		{"or", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{term("mk10"), term("mk1"), term("mk01")})
		}, 10},
		{"or5", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{term("mk50"), term("mk10"), term("mk1"), term("mk01"), term("w3")})
		}, 10},
	}
	for _, sh := range shapes {
		for _, path := range []struct {
			name    string
			enabled bool
		}{{"perseg", true}, {"regular", false}} {
			b.Run(sh.name+"/"+path.name, func(b *testing.B) {
				perSegmentSearchEnabled.Store(path.enabled)
				defer perSegmentSearchEnabled.Store(true)
				q := sh.q()
				// warm
				for i := 0; i < 20; i++ {
					if _, err := idx.Search(NewSearchRequestOptions(q, sh.size, 0, false)); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := idx.Search(NewSearchRequestOptions(q, sh.size, 0, false)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkSearchCPU is for CPU profiles of the per segment path: one shape at
// a time, BM25 or tf-idf, the way a profile of a running service would see it.
//
//	PERSEG_SHAPE=term|and|or PERSEG_MODEL=bm25 go test -run xxx -bench SearchCPU \
//	    -benchtime 20s -cpuprofile cpu.out
func BenchmarkSearchCPU(b *testing.B) {
	shape := os.Getenv("PERSEG_SHAPE")
	if shape == "" {
		b.Skip("set PERSEG_SHAPE")
	}
	idx, cleanup := buildAllocBenchIndexModel(b, 200000, 8, os.Getenv("PERSEG_MODEL"))
	defer cleanup()

	term := func(t string) query.Query {
		q := query.NewTermQuery(t)
		q.SetField("body")
		return q
	}
	var q query.Query
	switch shape {
	case "term":
		q = term("mk10")
	case "and":
		q = query.NewConjunctionQuery([]query.Query{term("w0"), term("w1"), term("w2")})
	case "or":
		q = query.NewDisjunctionQuery([]query.Query{term("w0"), term("w1"), term("w10"), term("w100"), term("mk1")})
	default:
		b.Fatalf("unknown shape %q", shape)
	}
	for i := 0; i < 50; i++ {
		if _, err := idx.Search(NewSearchRequestOptions(q, 10, 0, false)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(NewSearchRequestOptions(q, 10, 0, false)); err != nil {
			b.Fatal(err)
		}
	}
}
