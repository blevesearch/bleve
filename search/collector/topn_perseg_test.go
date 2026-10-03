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

package collector

import (
	"context"
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// The per segment collector knows nothing about terms, segments' postings or
// scorers: it needs a search.PerSegmentSearcher. These are made-up ones.

type fakeSeg struct {
	offset uint64
	docs   []uint32
	scores []float32
}

// fakeSearcher streams its segments' matches, one at a time.
type fakeSearcher struct {
	search.PerSegmentSearcher // not used
	segs                      []fakeSeg

	seg, pos int
}

func (f *fakeSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	for f.seg < len(f.segs) {
		s := f.segs[f.seg]
		if f.pos >= len(s.docs) {
			f.seg++
			f.pos = 0
			continue
		}
		m := search.PerSegmentMatch{Seg: f.seg, Doc: s.offset + uint64(s.docs[f.pos]), Score: s.scores[f.pos]}
		f.pos++
		return m, true, nil
	}
	return search.PerSegmentMatch{}, false, nil
}

func (f *fakeSearcher) Close() error { return nil }

// optimizedFake does the same, but by itself, through the sink
type optimizedFake struct {
	*fakeSearcher
	collected bool
}

func (o *optimizedFake) CanCollectOptimized() bool { return true }

func (o *optimizedFake) CollectOptimized(ctx context.Context, sink search.PerSegmentSink) error {
	o.collected = true
	for seg, s := range o.segs {
		h := sink.Heap(seg)
		for i, d := range s.docs {
			sink.ObserveMaxScore(s.scores[i])
			h.Offer(search.PerSegmentHit{Score: s.scores[i], Doc: s.offset + uint64(d), Ord: uint32(i), Seg: uint32(seg)})
		}
		sink.AddTotal(seg, uint64(len(s.docs)))
	}
	return nil
}

type declinesOptimized struct {
	*optimizedFake
}

func (d *declinesOptimized) CanCollectOptimized() bool { return false }

type fakeReader struct {
	index.IndexReader
}

func (fakeReader) ExternalID(id index.IndexInternalID) (string, error) {
	return fmt.Sprint(id.Value()), nil
}

func fakeSegments() []fakeSeg {
	// docs ascend in a segment; the scores have ties, within and across segments
	return []fakeSeg{
		{offset: 0, docs: []uint32{0, 1, 2, 3, 4, 5}, scores: []float32{1, 5, 3, 5, 2, 1}},
		{offset: 100, docs: []uint32{1, 2, 3}, scores: []float32{5, 4, 3}},
		{offset: 200, docs: nil, scores: nil},
		{offset: 300, docs: []uint32{0, 7}, scores: []float32{9, 5}},
	}
}

func TestPerSegmentCollectorIsGeneric(t *testing.T) {
	// best first, ties to the lower doc number
	wantAll := []struct {
		id    string
		score float64
	}{
		{"300", 9}, {"1", 5}, {"3", 5}, {"101", 5}, {"307", 5}, {"102", 4},
		{"2", 3}, {"103", 3}, {"4", 2}, {"0", 1}, {"5", 1},
	}

	for name, mk := range map[string]func() search.PerSegmentSearcher{
		"generic": func() search.PerSegmentSearcher { return &fakeSearcher{segs: fakeSegments()} },
		"optimized": func() search.PerSegmentSearcher {
			return &optimizedFake{fakeSearcher: &fakeSearcher{segs: fakeSegments()}}
		},
		"declines optimized": func() search.PerSegmentSearcher {
			return &declinesOptimized{&optimizedFake{fakeSearcher: &fakeSearcher{segs: fakeSegments()}}}
		},
	} {
		for _, sz := range []struct{ size, from int }{{3, 0}, {4, 2}, {100, 0}, {0, 0}, {2, 9}, {5, 10}, {1, 50}} {
			c := NewPerSegmentTopNCollector(sz.size, sz.from)
			if err := c.Collect(context.Background(), mk(), fakeReader{}); err != nil {
				t.Fatal(err)
			}

			want := wantAll
			if sz.from < len(want) {
				want = want[sz.from:]
			} else {
				want = nil
			}
			if len(want) > sz.size {
				want = want[:sz.size]
			}

			what := fmt.Sprintf("%s %+v", name, sz)
			if c.Total() != 11 || c.MaxScore() != 9 {
				t.Fatalf("%s: total %d max %v", what, c.Total(), c.MaxScore())
			}
			got := c.Results()
			if len(got) != len(want) {
				t.Fatalf("%s: %d hits, want %d", what, len(got), len(want))
			}
			for i := range want {
				if got[i].ID != want[i].id || got[i].Score != want[i].score {
					t.Fatalf("%s: hit %d is %s/%v, want %s/%v", what, i, got[i].ID, got[i].Score, want[i].id, want[i].score)
				}
			}
		}
	}
}

func TestPerSegmentCollectorUsesTheOptimizedPathWhenOffered(t *testing.T) {
	opt := &optimizedFake{fakeSearcher: &fakeSearcher{segs: fakeSegments()}}
	if err := NewPerSegmentTopNCollector(3, 0).Collect(context.Background(), opt, fakeReader{}); err != nil {
		t.Fatal(err)
	}
	if !opt.collected {
		t.Fatal("the optimized path wasn't used")
	}
	if opt.seg != 0 || opt.pos != 0 {
		t.Fatal("the searcher was drained as well")
	}

	declines := &declinesOptimized{&optimizedFake{fakeSearcher: &fakeSearcher{segs: fakeSegments()}}}
	if err := NewPerSegmentTopNCollector(3, 0).Collect(context.Background(), declines, fakeReader{}); err != nil {
		t.Fatal(err)
	}
	if declines.collected {
		t.Fatal("a searcher that can't collect optimized was asked to")
	}
}
