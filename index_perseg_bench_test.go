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
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// TestPerSegmentSearchBench compares the per segment path with the regular
// one on the same index. It is a no-op unless PERSEG_BENCH=1:
//
//	PERSEG_BENCH=1 go test -run TestPerSegmentSearchBench -v -timeout 30m .
//
// PERSEG_BENCH_DOCS (default 200000) and PERSEG_BENCH_BATCHES (default 8,
// which is the number of segments) size the index. PERSEG_BENCH_MODEL=bm25
// scores with bm25. PERSEG_BENCH_SHAPES=composites leaves the single terms out.
// PERSEG_BENCH_WARMUP, PERSEG_BENCH_ROUNDS and PERSEG_BENCH_QUERIES set the
// rounds thrown away, the rounds measured, and the queries run for a shape in
// each (defaults 3, 15 and 200).
//
// The two paths run interleaved, in rounds, with the order alternating from
// round to round, and the first rounds are thrown away as warm up.
func TestPerSegmentSearchBench(t *testing.T) {
	if os.Getenv("PERSEG_BENCH") != "1" {
		t.Skip("set PERSEG_BENCH=1 to run")
	}
	numDocs := envInt("PERSEG_BENCH_DOCS", 200000)
	numBatches := envInt("PERSEG_BENCH_BATCHES", 8)
	warmupRounds := envInt("PERSEG_BENCH_WARMUP", 3)
	measuredRounds := envInt("PERSEG_BENCH_ROUNDS", 15)
	queriesPerCell := envInt("PERSEG_BENCH_QUERIES", 200)

	dir := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, dir)
	im := NewIndexMapping()
	im.ScoringModel = index.DefaultScoringModel
	if os.Getenv("PERSEG_BENCH_MODEL") == "bm25" {
		im.ScoringModel = index.BM25Scoring
	}
	t.Logf("scoring model: %s", im.ScoringModel)
	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, map[string]interface{}{
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	// body: 3..40 words from a zipf-ish 5000 word vocabulary, plus marker
	// terms of known document frequency
	rnd := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rnd, 1.2, 4, 4999)
	markers := []struct {
		term string
		pct  float64 // percentage of documents having it
	}{{"mk100", 100}, {"mk50", 50}, {"mk10", 10}, {"mk1", 1}, {"mk01", 0.1}}

	t.Logf("indexing %d docs in %d segments...", numDocs, numBatches)
	start := time.Now()
	per := numDocs / numBatches
	for bt := 0; bt < numBatches; bt++ {
		b := idx.NewBatch()
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
			if err := b.Index(strconv.Itoa(i), map[string]interface{}{"body": sb.String()}); err != nil {
				t.Fatal(err)
			}
		}
		persisted := make(chan error, 1)
		b.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
		if err := <-persisted; err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := segmentCountAndDeletions(t, idx)
	t.Logf("indexed in %v, %d segments", time.Since(start).Round(time.Millisecond), segs)

	type cell struct {
		name string
		q    func() query.Query
		size int
		none bool // Score: "none"
	}
	termQ := func(term string) func() query.Query {
		return func() query.Query {
			tq := query.NewTermQuery(term)
			tq.SetField("body")
			return tq
		}
	}
	terms := func(names ...string) []query.Query {
		rv := make([]query.Query, len(names))
		for i, n := range names {
			tq := query.NewTermQuery(n)
			tq.SetField("body")
			rv[i] = tq
		}
		return rv
	}
	orQ := func(names ...string) func() query.Query {
		return func() query.Query { return query.NewDisjunctionQuery(terms(names...)) }
	}
	andQ := func(names ...string) func() query.Query {
		return func() query.Query { return query.NewConjunctionQuery(terms(names...)) }
	}
	matchQ := func(text string, op query.MatchQueryOperator) func() query.Query {
		return func() query.Query {
			mq := query.NewMatchQuery(text)
			mq.SetField("body")
			mq.SetOperator(op)
			return mq
		}
	}

	var cells []cell
	if os.Getenv("PERSEG_BENCH_SHAPES") != "composites" {
		for _, m := range markers {
			for _, size := range []int{10, 100} {
				cells = append(cells, cell{m.term, termQ(m.term), size, false})
			}
			cells = append(cells, cell{m.term, termQ(m.term), 10, true})
			cells = append(cells, cell{m.term, termQ(m.term), 0, true}) // the count query
		}
	}
	composites := []struct {
		name string
		q    func() query.Query
	}{
		{"and-high-low", andQ("mk100", "mk1")},
		{"and-mid-mid", andQ("mk50", "mk10")},
		{"and-zipf-2", andQ("w0", "w1")},
		{"and-zipf-3", andQ("w0", "w1", "w2")},
		{"or-2-high", orQ("mk100", "mk50")},
		{"or-5-mid", orQ("mk50", "mk10", "mk1", "mk01", "mk100")},
		{"or-zipf-2", orQ("w0", "w1")},
		{"or-zipf-5", orQ("w0", "w1", "w2", "w3", "w4")},
		{"or-zipf-mix", orQ("w0", "w10", "w100", "w1000", "mk1")},
		{"match-or-3", matchQ("w2 w30 w400", query.MatchQueryOperatorOr)},
		{"match-and-2", matchQ("w2 w30", query.MatchQueryOperatorAnd)},
	}
	for _, c := range composites {
		cells = append(cells, cell{c.name, c.q, 10, false})
		cells = append(cells, cell{c.name, c.q, 100, false})
		cells = append(cells, cell{c.name, c.q, 10, true})
		cells = append(cells, cell{c.name, c.q, 0, true})
	}

	runCell := func(c cell, perSegment bool) []time.Duration {
		perSegmentSearchEnabled.Store(perSegment)
		tq := c.q()
		lats := make([]time.Duration, 0, queriesPerCell)
		for q := 0; q < queriesPerCell; q++ {
			req := NewSearchRequestOptions(tq, c.size, 0, false)
			if c.none {
				req.Score = ScoreNone
			}
			t0 := time.Now()
			res, err := idx.Search(req)
			lats = append(lats, time.Since(t0))
			if err != nil {
				t.Fatal(err)
			}
			if res.Total == 0 {
				t.Fatalf("no hits for %s", c.name)
			}
		}
		return lats
	}
	defer perSegmentSearchEnabled.Store(true)

	t.Logf("%-22s %-9s %9s %9s %9s %10s | %9s %9s %9s %10s | %7s %7s", "shape", "path",
		"p50", "p95", "p99", "qps", "", "", "", "", "p50x", "qpsx")
	for _, c := range cells {
		var oldAll, newAll []time.Duration
		for round := 0; round < warmupRounds+measuredRounds; round++ {
			// alternate who goes first, to cancel out drift
			order := []bool{false, true}
			if round%2 == 1 {
				order = []bool{true, false}
			}
			for _, perSegment := range order {
				lats := runCell(c, perSegment)
				if round < warmupRounds {
					continue
				}
				if perSegment {
					newAll = append(newAll, lats...)
				} else {
					oldAll = append(oldAll, lats...)
				}
			}
		}
		o, n := summarize(oldAll), summarize(newAll)
		shape := fmt.Sprintf("%s size=%d", c.name, c.size)
		if c.none {
			shape += " none"
		}
		t.Logf("%-22s %-9s %9v %9v %9v %10.0f", shape, "regular", o.p50, o.p95, o.p99, o.qps)
		t.Logf("%-22s %-9s %9v %9v %9v %10.0f | %46s %6.2fx %6.2fx", shape, "per-seg", n.p50, n.p95, n.p99, n.qps, "",
			float64(o.p50)/float64(n.p50), n.qps/o.qps)
	}
}

type latSummary struct {
	p50, p95, p99 time.Duration
	qps           float64
}

func summarize(lats []time.Duration) latSummary {
	sorted := append([]time.Duration(nil), lats...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		return sorted[int(float64(len(sorted)-1)*p)].Round(time.Microsecond)
	}
	var total time.Duration
	for _, l := range lats {
		total += l
	}
	return latSummary{
		p50: pct(0.50), p95: pct(0.95), p99: pct(0.99),
		qps: float64(len(lats)) / total.Seconds(),
	}
}

func envInt(name string, dflt int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return dflt
}
