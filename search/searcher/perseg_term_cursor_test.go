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
	"fmt"
	"math"
	"math/rand"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

type cursorPosting struct {
	doc   uint32
	score float32
}

// postingsOf drains the readers of a segment the plain way: block after block,
// scored by the block kernel. It is what cursors are checked against.
func postingsOf(t *testing.T, s *PerSegmentTermSearcher, seg int) []cursorPosting {
	t.Helper()
	r := s.Readers()[seg]
	if r == nil {
		return nil
	}
	var rv []cursorPosting
	var scores [128]float32
	for {
		blk, n, err := r.NextBlock()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return rv
		}
		s.Scorer().ScoreBlock(&blk.Freqs, &blk.Norms, n, &scores)
		for i := 0; i < n; i++ {
			rv = append(rv, cursorPosting{blk.Docs[i], scores[i]})
		}
	}
}

func forEachFixtureTerm(t *testing.T, f func(t *testing.T, fx *perSegFixture, term, model string)) {
	for _, deleteEvery := range []int{0, 7} {
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 4, docsPerSeg: 900, deleteEvery: deleteEvery, seed: 5})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, tm := range perSegFixtureTerms {
				t.Run(fmt.Sprintf("deleteEvery=%d/%s/%s", deleteEvery, model, tm.term), func(t *testing.T) {
					f(t, fx, tm.term, model)
				})
			}
		}
	}
}

func TestTermCursorWalksThePostings(t *testing.T) {
	forEachFixtureTerm(t, func(t *testing.T, fx *perSegFixture, term, model string) {
		oracle := fx.termSearcher(term, true, model)
		cursors := fx.termSearcher(term, true, model)
		defer func() { _ = oracle.Close(); _ = cursors.Close() }()

		for seg, r := range cursors.Readers() {
			if r == nil {
				continue
			}
			want := postingsOf(t, oracle, seg)
			c := newTermCursor(0, r, cursors.Scorer(), true)
			var got []cursorPosting
			for d := c.Doc(); d != noMoreDocs; d = c.Advance() {
				got = append(got, cursorPosting{d, c.Score()})
			}
			if c.Err() != nil {
				t.Fatal(c.Err())
			}
			if len(got) != len(want) {
				t.Fatalf("seg %d: %d postings, want %d", seg, len(got), len(want))
			}
			for i := range want {
				if math.Float32bits(got[i].score) != math.Float32bits(want[i].score) || got[i].doc != want[i].doc {
					t.Fatalf("seg %d posting %d: %+v, want %+v", seg, i, got[i], want[i])
				}
			}
		}
	})
}

func TestTermCursorSeek(t *testing.T) {
	forEachFixtureTerm(t, func(t *testing.T, fx *perSegFixture, term, model string) {
		oracle := fx.termSearcher(term, true, model)
		cursors := fx.termSearcher(term, true, model)
		defer func() { _ = oracle.Close(); _ = cursors.Close() }()
		rnd := rand.New(rand.NewSource(77))

		for seg, r := range cursors.Readers() {
			if r == nil {
				continue
			}
			want := postingsOf(t, oracle, seg)
			c := newTermCursor(0, r, cursors.Scorer(), true)

			// jump about, always forward, by steps of every size
			var target uint32
			for trial := 0; trial < 200 && c.Doc() != noMoreDocs; trial++ {
				switch rnd.Intn(4) {
				case 0:
					target = c.Doc() + 1
				case 1:
					target = c.Doc() + uint32(rnd.Intn(10))
				case 2:
					target = c.Doc() + uint32(rnd.Intn(400))
				default:
					target = c.Doc() + uint32(rnd.Intn(5))
				}
				var wantDoc = noMoreDocs
				var wantScore float32
				for _, p := range want {
					if p.doc >= target {
						wantDoc, wantScore = p.doc, p.score
						break
					}
				}
				if got := c.Seek(target); got != wantDoc {
					t.Fatalf("seg %d: seek %d gave %d, want %d", seg, target, got, wantDoc)
				}
				if wantDoc != noMoreDocs && math.Float32bits(c.Score()) != math.Float32bits(wantScore) {
					t.Fatalf("seg %d: seek %d score %v, want %v", seg, target, c.Score(), wantScore)
				}
			}
		}
	})
}

// ShallowSeek bounds the scores of the block that has the target without
// disturbing the cursor, however it's mixed with Advance and Seek
func TestTermCursorShallowSeek(t *testing.T) {
	forEachFixtureTerm(t, func(t *testing.T, fx *perSegFixture, term, model string) {
		oracle := fx.termSearcher(term, true, model)
		cursors := fx.termSearcher(term, true, model)
		defer func() { _ = oracle.Close(); _ = cursors.Close() }()
		rnd := rand.New(rand.NewSource(91))

		for seg, r := range cursors.Readers() {
			if r == nil {
				continue
			}
			if !r.HasBlockMax() {
				t.Fatal("expected block max data")
			}
			want := postingsOf(t, oracle, seg)
			c := newTermCursor(0, r, cursors.Scorer(), true)
			if c.MaxScore() < func() float32 {
				var m float32
				for _, p := range want {
					m = max(m, p.score)
				}
				return m
			}() {
				t.Fatalf("seg %d: max score %v below a posting's", seg, c.MaxScore())
			}

			pos := 0 // where the cursor is, in want
			for c.Doc() != noMoreDocs {
				if c.Doc() != want[pos].doc {
					t.Fatalf("seg %d: cursor on %d, want %d", seg, c.Doc(), want[pos].doc)
				}

				// look at faraway blocks
				for k := rnd.Intn(4); k > 0; k-- {
					target := c.Doc() + uint32(rnd.Intn(1500))
					c.ShallowSeek(target)
					last := c.LastDocInBlock()
					if last != noMoreDocs && last < target {
						t.Fatalf("seg %d: block of %d ends at %d", seg, target, last)
					}
					bm := c.BlockMaxScore()
					// the bound holds for every posting from target to the block's end
					for _, p := range want {
						if p.doc >= target && p.doc <= last && p.score > bm {
							t.Fatalf("seg %d: doc %d scores %v above the block max %v of the block %d..%d",
								seg, p.doc, p.score, bm, target, last)
						}
					}
					if last == noMoreDocs {
						for _, p := range want {
							if p.doc >= target {
								t.Fatalf("seg %d: no block for %d, but doc %d exists", seg, target, p.doc)
							}
						}
					}
				}

				// carry on, one way or the other
				if rnd.Intn(3) == 0 {
					step := 1 + rnd.Intn(60)
					wantPos := min(pos+step, len(want))
					target := want[min(wantPos, len(want)-1)].doc
					if wantPos >= len(want) {
						target = c.Doc() + 100000
					}
					c.Seek(target)
					pos = wantPos
				} else {
					c.Advance()
					pos++
				}
				if pos >= len(want) {
					if c.Doc() != noMoreDocs {
						t.Fatalf("seg %d: cursor on %d past the last posting", seg, c.Doc())
					}
				}
			}
			if pos < len(want) {
				t.Fatalf("seg %d: the cursor stopped at posting %d of %d", seg, pos, len(want))
			}
		}
	})
}
