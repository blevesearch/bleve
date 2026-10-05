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
	"math/rand"
	"testing"

	"github.com/blevesearch/bleve/v2/search/scorer"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// fakeBlocks is a term's blocks, with the bounds of each.
type fakeBlocks []segment.BlockBounds

func (f fakeBlocks) BoundsAt(target uint32) (segment.BlockBounds, bool) {
	for _, b := range f {
		if b.LastDoc >= target {
			return b, true
		}
	}
	return segment.BlockBounds{LastDoc: noMoreDocs}, false
}

// boundUpTo is the best of the bounds of every block from the one that holds
// start up to the one that holds target, and the end of the last of them. Blocks
// that differ a lot, as those of a term whose best docs are in a few places do,
// are what tells it from the bound of the first block alone.
func TestBoundUpToIsTheBestOfTheBlocksInTheRange(t *testing.T) {
	sc := scorer.NewPerSegmentTermScorer(1, 100000, 5000, 0)
	rnd := rand.New(rand.NewSource(4))
	for trial := 0; trial < 300; trial++ {
		var blocks fakeBlocks
		last := uint32(0)
		for i, nb := 0, 1+rnd.Intn(30); i < nb; i++ {
			last += uint32(100 + rnd.Intn(900))
			blocks = append(blocks, segment.BlockBounds{
				LastDoc:     last,
				MaxFreq:     uint32(1 + rnd.Intn(20)),
				MaxNorm:     0.1 + rnd.Float32()*0.9,
				FreqBounded: true,
			})
		}
		start := uint32(rnd.Intn(int(last) + 500))
		target := uint64(start) + uint64(rnd.Intn(8000))

		// by hand: the blocks from the first with a last doc >= start, to the
		// first with one >= target (or the end)
		var want float32
		var wantLast uint32
		found := false
		for _, b := range blocks {
			if b.LastDoc < start {
				continue
			}
			ub := sc.UpperBound(b.MaxFreq, b.MaxNorm, b.FreqBounded)
			if !found || ub > want {
				want = ub
			}
			found = true
			wantLast = b.LastDoc
			if uint64(b.LastDoc) >= target {
				break
			}
		}

		got, gotLast, ok := boundUpTo(blocks, sc, start, target)
		if ok != found {
			t.Fatalf("trial %d: ok=%v, want %v", trial, ok, found)
		}
		if !ok {
			continue
		}
		if got != want || gotLast != wantLast {
			t.Fatalf("trial %d start=%d target=%d: bound %v last %d, want %v last %d",
				trial, start, target, got, gotLast, want, wantLast)
		}
	}
}
