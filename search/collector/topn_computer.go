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
	"bytes"
	"math/bits"
	"sort"
)

// The top N of a search sorted by something other than the score: hits are kept
// the way tantivy's TopNComputer keeps them. There is no pruning to feed here, so
// there is no use for a threshold that is always the exact N-th best (which is
// what a heap is for); what matters is the cost of a hit that doesn't make it, and
// of one that does.
//
//   - a hit that doesn't beat the threshold is turned down by one comparison;
//   - one that does is appended to a buffer of 2N hits, which costs nothing more;
//   - when the buffer is full, the best N are selected out of it (a quickselect,
//     O(N), so O(1) a hit that was appended) and the rest are thrown away. The
//     best of those thrown away is the new threshold.
//
// Hits have to be offered in ascending order of when they were found (ord). That
// is what makes the threshold strict: a hit that ties it ranks below it, as it
// was found after, so it doesn't need to be compared by anything but its sort
// values.

// sortedHit is a hit of a search sorted by sort values.
type sortedHit struct {
	doc uint64
	// ord is the 1-based place of the hit among the matches of the search, in the
	// order they were found: the HitNumber of the regular collector, and what
	// ties between sort values are decided by (the earlier ranks first).
	ord   uint64
	score float32
	seg   uint32
	// keys are the sort values, in the order of the sorts that aren't by score.
	keys [][]byte
}

// sortComponent is one of the sorts of a sort order.
type sortComponent struct {
	desc  bool
	score bool // the sort is by score, which is in the hit; otherwise key is the place in keys
	key   int
}

// sortRanking is how hits of a sort order rank.
type sortRanking []sortComponent

// compare is negative if a ranks before b, and positive if after. It is never 0
// for hits that were found at different times.
func (r sortRanking) compare(a, b *sortedHit) int {
	for _, c := range r {
		var x int
		if c.score {
			if a.score < b.score {
				x = -1
			} else if a.score > b.score {
				x = 1
			}
		} else {
			x = bytes.Compare(a.keys[c.key], b.keys[c.key])
		}
		if x == 0 {
			continue
		}
		if c.desc {
			x = -x
		}
		return x
	}
	if a.ord < b.ord {
		return -1
	}
	if a.ord > b.ord {
		return 1
	}
	return 0
}

// sortedTopN keeps the best n hits offered to it.
type sortedTopN struct {
	n    int
	rank sortRanking
	buf  []sortedHit // up to 2n hits, in no order

	threshold    sortedHit
	hasThreshold bool
}

// sortedTopNPreAlloc caps what is allocated up front.
const sortedTopNPreAlloc = 1000

func newSortedTopN(n int, rank sortRanking) *sortedTopN {
	c := 2 * max(n, 1)
	return &sortedTopN{n: n, rank: rank, buf: make([]sortedHit, 0, min(c, 2*sortedTopNPreAlloc))}
}

// beats reports whether a hit that was found after all those offered so far would
// be kept, going by the threshold. A hit that doesn't gets rejected without being
// given to add.
func (t *sortedTopN) beats(h *sortedHit) bool {
	return !t.hasThreshold || t.rank.compare(h, &t.threshold) < 0
}

// add keeps the hit, which has to have been checked with beats. The hit's keys
// must be its own: they're kept.
func (t *sortedTopN) add(h sortedHit) {
	if len(t.buf) >= 2*max(t.n, 1) {
		t.truncate()
	}
	t.buf = append(t.buf, h)
}

// truncate keeps the best n hits of the buffer, and the best of those it drops
// becomes the threshold.
func (t *sortedTopN) truncate() {
	if len(t.buf) <= t.n {
		return
	}
	selectNth(t.buf, t.n, t.rank)
	t.threshold = t.buf[t.n]
	t.hasThreshold = true
	// let go of the keys of the hits dropped
	for i := t.n; i < len(t.buf); i++ {
		t.buf[i] = sortedHit{}
	}
	t.buf = t.buf[:t.n]
}

// sorted returns the best n hits, best first.
func (t *sortedTopN) sorted() []sortedHit {
	t.truncate()
	r := t.rank
	sort.Slice(t.buf, func(i, j int) bool { return r.compare(&t.buf[i], &t.buf[j]) < 0 })
	return t.buf
}

// selectNth rearranges a so that the n best hits come first and the n-th (counting
// from 0) is the one that would be there if a were sorted. Hits are all
// different, as they have been found at different times. A bad run of pivots
// falls back to sorting what's left.
func selectNth(a []sortedHit, n int, r sortRanking) {
	lo, hi := 0, len(a)-1
	budget := 2 * bits.Len(uint(len(a)))
	for lo < hi {
		budget--
		if budget < 0 {
			s := a[lo : hi+1]
			sort.Slice(s, func(i, j int) bool { return r.compare(&s[i], &s[j]) < 0 })
			return
		}
		mid := lo + (hi-lo)/2
		if r.compare(&a[mid], &a[lo]) < 0 {
			a[mid], a[lo] = a[lo], a[mid]
		}
		if r.compare(&a[hi], &a[lo]) < 0 {
			a[hi], a[lo] = a[lo], a[hi]
		}
		if r.compare(&a[hi], &a[mid]) < 0 {
			a[hi], a[mid] = a[mid], a[hi]
		}
		pivot := a[mid]
		i, j := lo, hi
		for i <= j {
			for r.compare(&a[i], &pivot) < 0 {
				i++
			}
			for r.compare(&pivot, &a[j]) < 0 {
				j--
			}
			if i <= j {
				a[i], a[j] = a[j], a[i]
				i++
				j--
			}
		}
		switch {
		case n <= j:
			hi = j
		case n >= i:
			lo = i
		default:
			return
		}
	}
}
