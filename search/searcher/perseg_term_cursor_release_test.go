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
	"sync"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// readBlockScored reads a term cursor through to its end, with the scores that
// the windows use: those of the whole block, worked out by the block kernel in the
// cursor's buffer (Score, of one posting, doesn't use it). every(i, c) is called
// before the i-th posting is read.
func readBlockScored(c *termCursor, every func(i int, c *termCursor)) []cursorPosting {
	var rv []cursorPosting
	for i := 0; c.Doc() != noMoreDocs; i++ {
		if every != nil {
			every(i, c)
		}
		rv = append(rv, cursorPosting{c.Doc(), c.blockScores()[c.pos]})
		c.Advance()
	}
	return rv
}

// A term cursor that is released gives back the buffer its scores are worked out
// in, and is harmless: released now and then in the middle of being read, and more
// than once, it gives the scores it would have given if it hadn't been (it takes a
// buffer again, and works the block's scores out again), whatever else is using
// the pool meanwhile.
func TestTermCursorReleaseIsHarmless(t *testing.T) {
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 3000, seed: 5, deleteEvery: 11})
	for _, model := range []string{"", index.BM25Scoring} {
		for _, term := range []string{"alpha", "bravo", "charlie", "echo"} {
			for seg := 0; seg < 3; seg++ {
				fresh := func() *termCursor {
					c, ok := fx.termSearcher(term, true, model).segCursor(seg, true)
					if !ok {
						t.Fatalf("%s: no cursor in segment %d", term, seg)
					}
					return c.(*termCursor)
				}
				// what the plain block by block read gives, the reference of the
				// cursor tests
				want := postingsOf(t, fx.termSearcher(term, true, model), seg)
				if len(want) < 16 {
					continue
				}

				released := 0
				c := fresh()
				got := readBlockScored(c, func(i int, tc *termCursor) {
					if i%8 == 7 {
						if tc.scores == nil {
							t.Fatalf("%s seg %d: the cursor has scored nothing: the test proves nothing", term, seg)
						}
						releaseCursor(tc)
						releaseCursor(tc) // twice
						if tc.scores != nil {
							t.Fatalf("%s seg %d: a released cursor still holds its buffer", term, seg)
						}
						// whatever else is working in the pool's buffers meanwhile (the
						// one just given back, most likely) overwrites them
						for k := 0; k < 4; k++ {
							b := scoreBufPool.Get().(*[128]float32)
							for j := range b {
								b[j] = -1
							}
							scoreBufPool.Put(b)
						}
						released++
					}
				})
				if released == 0 {
					t.Fatalf("%s seg %d: nothing was released: the test proves nothing", term, seg)
				}
				if len(got) != len(want) {
					t.Fatalf("%s seg %d: %d postings, want %d", term, seg, len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s seg %d: posting %d is %+v after releases, want %+v", term, seg, i, got[i], want[i])
					}
				}
			}
		}
	}
}

// Cursors of different goroutines, released all the time and so passing the same
// few buffers between them, never see one another's scores. (Run with -race.)
func TestTermCursorBuffersAreNotSharedBetweenCursorsInUse(t *testing.T) {
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 2, docsPerSeg: 3000, seed: 9})
	terms := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	want := map[string][]cursorPosting{}
	for _, term := range terms {
		want[term] = postingsOf(t, fx.termSearcher(term, true, ""), 0)
	}

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				term := terms[(g+round)%len(terms)]
				c, _ := fx.termSearcher(term, true, "").segCursor(0, true)
				tc := c.(*termCursor)
				got := readBlockScored(tc, func(i int, tc *termCursor) {
					if i%16 == 15 {
						releaseCursor(tc)
					}
				})
				releaseCursor(tc)
				w := want[term]
				if len(got) != len(w) {
					errs <- term + ": wrong number of postings"
					return
				}
				for i := range w {
					if got[i] != w[i] {
						errs <- term + ": a posting scored differently while buffers were being passed around"
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}
