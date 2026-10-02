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

package searcher

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// perSegFixture is a scorch index of many documents spread over several
// segments, with terms of very different frequencies, so that postings lists
// of every shape (many blocks, a tail, a single block, a single doc) exist.
type perSegFixture struct {
	t        *testing.T
	idx      index.Index
	snapshot *scorch.IndexSnapshot
	reader   index.IndexReader
	numDocs  uint64
}

type perSegFixtureOpts struct {
	segments    int
	docsPerSeg  int
	deleteEvery int // delete one in this many docs, if > 0
	seed        int64
	// outlierOneIn: one in this many postings has a frequency far above the
	// rest (default 40). The sparser they are, the more the blocks of a list
	// differ in how much they can score, which block-max pruning feeds on.
	outlierOneIn int
	// docTokens, if set, decides the tokens of each doc instead of the random
	// terms (the fillers are still added). It gets the segment and the place
	// of the doc in it.
	docTokens func(seg, i int) []string
}

// the terms, and the share of the documents (in 1/1000) that has each
var perSegFixtureTerms = []struct {
	term  string
	share int
}{
	{"alpha", 1000}, // every doc
	{"bravo", 500},
	{"charlie", 200},
	{"delta", 60},
	{"echo", 12},
	{"foxtrot", 2}, // a handful in all
}

func newPerSegFixture(t *testing.T, o perSegFixtureOpts) *perSegFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "perseg-fixture")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	idx, err := scorch.NewScorch(scorch.Name, map[string]interface{}{
		"path": dir,
		// the planner only merges segments smaller than half of this: none
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	}, index.NewAnalysisQueue(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	if o.outlierOneIn == 0 {
		o.outlierOneIn = 40
	}
	rnd := rand.New(rand.NewSource(o.seed))
	fillers := []string{"w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9"}

	apply := func(b *index.Batch) {
		persisted := make(chan error, 1)
		b.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
		if err := <-persisted; err != nil {
			t.Fatal(err)
		}
	}

	f := &perSegFixture{t: t, idx: idx}
	var ids []string
	for seg := 0; seg < o.segments; seg++ {
		b := index.NewBatch()
		for i := 0; i < o.docsPerSeg; i++ {
			id := fmt.Sprintf("d%d_%d", seg, i)
			ids = append(ids, id)
			var toks []string
			if o.docTokens != nil {
				toks = o.docTokens(seg, i)
			}
			for _, tm := range perSegFixtureTerms {
				if o.docTokens != nil {
					break
				}
				if rnd.Intn(1000) < tm.share {
					// frequencies of 1 mostly, now and then a lot
					reps := 1 + rnd.Intn(3)
					if rnd.Intn(o.outlierOneIn) == 0 {
						reps = 1 + rnd.Intn(300)
					}
					for r := 0; r < reps; r++ {
						toks = append(toks, tm.term)
					}
				}
			}
			for k := rnd.Intn(30); k > 0; k-- {
				toks = append(toks, fillers[rnd.Intn(len(fillers))])
			}
			rnd.Shuffle(len(toks), func(a, b int) { toks[a], toks[b] = toks[b], toks[a] })
			doc := document.NewDocument(id)
			doc.AddField(document.NewTextFieldCustom("body", []uint64{}, []byte(strings.Join(toks, " ")),
				document.DefaultTextIndexingOptions, testAnalyzer))
			b.Update(doc)
		}
		apply(b)
	}
	if o.deleteEvery > 0 {
		b := index.NewBatch()
		for i, id := range ids {
			if i%o.deleteEvery == 0 {
				b.Delete(id)
			}
		}
		apply(b)
	}

	r, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	f.reader = r
	f.snapshot = r.(*scorch.IndexSnapshot)
	f.numDocs, _ = r.DocCount()
	return f
}

// termSearcher is the per segment searcher of a term of the fixture.
func (f *perSegFixture) termSearcher(term string, scored bool, model string) *PerSegmentTermSearcher {
	f.t.Helper()
	ctx := context.Background()
	if model == index.BM25Scoring {
		ctx = context.WithValue(ctx, search.GetScoringModelCallbackKey,
			search.GetScoringModelCallbackFn(func() string { return index.BM25Scoring }))
	}
	opts := search.SearcherOptions{}
	if !scored {
		opts.Score = "none"
	}
	s, err := NewPerSegmentTermSearcher(ctx, f.snapshot, term, "body", 1.0, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

// conjunction and disjunction build the composite searchers of the clauses.
func (f *perSegFixture) conjunction(clauses []search.PerSegmentSearcher,
	opts search.SearcherOptions) *PerSegmentConjunctionSearcher {
	f.t.Helper()
	s, err := NewPerSegmentConjunctionSearcher(clauses, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *perSegFixture) disjunction(clauses []search.PerSegmentSearcher, min float64,
	opts search.SearcherOptions) *PerSegmentDisjunctionSearcher {
	f.t.Helper()
	s, err := NewPerSegmentDisjunctionSearcher(clauses, min, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}
