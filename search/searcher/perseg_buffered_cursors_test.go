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
	"math/rand"
	"runtime"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// The buffered cursors move like the cursors they stand for -- unionCursor and
// intersectionCursor, which share nothing with them but the term cursors -- under
// any mix of Advance and Seek: far, near, to a doc that is a match or not, to one
// in the window or beyond it. Doc and score are the same at every step.
//
// Cursors are also abandoned half way and released, one after the other, on a single
// P so that a pool hands the next cursor the memory the last one gave back: if a
// release left anything behind, the next cursor would have it in its scores.
func TestPerSegmentBufferedCursorsMoveLikeTheClassicOnes(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	sets := [][]string{
		{"alpha", "bravo"}, {"alpha", "bravo", "charlie"}, {"bravo", "charlie", "delta"},
		{"charlie", "delta"}, {"alpha", "delta", "echo"}, {"delta", "echo"}, {"bravo", "echo", "foxtrot"},
	}
	for _, deleteEvery := range []int{0, 7} {
		// segments of more than a window, so that the windows are crossed
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 2, docsPerSeg: 9000,
			deleteEvery: deleteEvery, seed: int64(40 + deleteEvery), outlierOneIn: 150})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, scored := range []bool{true, false} {
				for _, terms := range sets {
					for _, union := range []bool{true, false} {
						for seed := int64(1); seed <= 6; seed++ {
							what := fmt.Sprintf("deleteEvery=%d %s scored=%v union=%v %v seed=%d",
								deleteEvery, model, scored, union, terms, seed)
							checkBufferedCursor(t, what, fx, terms, model, scored, union, seed)
						}
					}
				}
			}
		}
	}
}

func checkBufferedCursor(t *testing.T, what string, fx *perSegFixture, terms []string, model string,
	scored, union bool, seed int64) {
	t.Helper()
	build := func() (ts []*PerSegmentTermSearcher) {
		for _, term := range terms {
			ts = append(ts, fx.termSearcher(term, scored, model))
		}
		return ts
	}
	for seg := 0; seg < len(fx.snapshot.Segments()); seg++ {
		tsA, tsB := build(), build()
		var buffered, classic docCursor
		if union {
			curs := buildTermCursors(tsA, seg, scored, false)
			if len(curs) == 0 {
				continue
			}
			buffered = newBufferedUnionCursor(curs, len(terms), scored)
			var cs []docCursor
			for _, c := range buildTermCursors(tsB, seg, scored, false) {
				cs = append(cs, c)
			}
			classic = newUnionCursor(cs, len(terms), 1)
		} else {
			curs := buildTermCursors(tsA, seg, scored, true)
			if curs == nil {
				continue
			}
			buffered = newBufferedIntersectionCursor(curs, scored)
			var cs []docCursor
			for _, c := range buildTermCursors(tsB, seg, scored, true) {
				cs = append(cs, c)
			}
			classic = newIntersectionCursor(cs)
		}

		rnd := rand.New(rand.NewSource(seed))
		// half of the cursors are given up on after a while
		stopAfter := -1
		if rnd.Intn(2) == 0 {
			stopAfter = rnd.Intn(300)
		}
		for step := 0; classic.Doc() != noMoreDocs; step++ {
			if step == stopAfter {
				releaseCursor(buffered)
				releaseCursor(classic)
				break
			}
			if buffered.Doc() != classic.Doc() {
				t.Fatalf("%s seg %d step %d: doc %d, want %d", what, seg, step, buffered.Doc(), classic.Doc())
			}
			if scored && buffered.Score() != classic.Score() {
				t.Fatalf("%s seg %d step %d doc %d: score %v, want %v", what, seg, step,
					classic.Doc(), buffered.Score(), classic.Score())
			}
			switch r := rnd.Intn(20); {
			case r == 0:
				// a long run of Advance, with which a cursor that fell back comes back
				for i := rnd.Intn(150); i > 0 && classic.Doc() != noMoreDocs; i-- {
					buffered.Advance()
					classic.Advance()
					if buffered.Doc() != classic.Doc() {
						t.Fatalf("%s seg %d: in a run of Advance, doc %d, want %d", what, seg,
							buffered.Doc(), classic.Doc())
					}
				}
			case r < 8:
				buffered.Advance()
				classic.Advance()
			default:
				// a jump: a few docs, a window, or many windows; or to where it is
				d := classic.Doc()
				var jump uint32
				switch r % 3 {
				case 0:
					jump = uint32(rnd.Intn(70))
				case 1:
					jump = uint32(rnd.Intn(4500))
				default:
					jump = uint32(rnd.Intn(30000))
				}
				if d+jump < d { // overflow
					jump = 0
				}
				buffered.Seek(d + jump)
				classic.Seek(d + jump)
			}
		}
		if stopAfter >= 0 && classic.Doc() != noMoreDocs {
			continue // given up on
		}
		if buffered.Doc() != noMoreDocs {
			t.Fatalf("%s seg %d: the buffered cursor is on %d, the classic one is done", what, seg, buffered.Doc())
		}
		releaseCursor(buffered)
		releaseCursor(classic)
	}
}

// A union that is released goes back to its pool as the pool expects it, all zeros,
// wherever it was left: at the start, in the middle of a window, after a seek, after
// it fell back to the unionCursor and came back, or done.
func TestPerSegmentBufferedUnionReleaseLeavesTheScratchClean(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1)) // a pool hands back what was put in it on the same P
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 1, docsPerSeg: 9000, seed: 51, outlierOneIn: 150})
	terms := []string{"alpha", "bravo", "charlie"}

	clean := func(what string) {
		t.Helper()
		s := bufUnionScratchPool.Get().(*bufUnionScratch)
		defer bufUnionScratchPool.Put(s)
		for i := range s.scores {
			if s.scores[i] != 0 || s.counts[i] != 0 {
				t.Fatalf("%s: slot %d is %v/%d", what, i, s.scores[i], s.counts[i])
			}
		}
		for w, word := range s.words {
			if word != 0 {
				t.Fatalf("%s: word %d is %x", what, w, word)
			}
		}
	}
	newUnion := func() *bufferedUnionCursor {
		var ts []*PerSegmentTermSearcher
		for _, term := range terms {
			ts = append(ts, fx.termSearcher(term, true, index.BM25Scoring))
		}
		return newBufferedUnionCursor(buildTermCursors(ts, 0, true, false), len(terms), true)
	}

	for _, advances := range []int{0, 1, 7, 64, 300, 2000, 100000} {
		u := newUnion()
		for i := 0; i < advances && u.Doc() != noMoreDocs; i++ {
			u.Advance()
		}
		u.release()
		clean(fmt.Sprintf("after %d advances", advances))
	}
	for _, jump := range []uint32{3, 100, 1000, 5000, 20000} {
		u := newUnion()
		u.Advance()
		u.Seek(u.Doc() + jump)
		u.release()
		clean(fmt.Sprintf("after a seek of %d", jump))
	}
	// fell back and came back
	u := newUnion()
	u.Seek(u.Doc() + 6000)
	for i := 0; i < 3*fallbackAdvances; i++ {
		u.Advance()
	}
	u.release()
	clean("after falling back and coming back")
}
