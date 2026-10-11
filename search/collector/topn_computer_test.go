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
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// randomHits makes hits found one after the other, with sort values from a
// small alphabet so that ties on them are the rule, not the exception.
func randomHits(rnd *rand.Rand, count, nkeys, alphabet int) []sortedHit {
	hits := make([]sortedHit, count)
	for i := range hits {
		h := sortedHit{doc: uint64(i * 3), ord: uint64(i + 1), score: float32(rnd.Intn(alphabet)), keys: make([][]byte, nkeys)}
		for k := range h.keys {
			h.keys[k] = []byte{byte('a' + rnd.Intn(alphabet))}
		}
		hits[i] = h
	}
	return hits
}

// oracleTopN is the best n of the hits by sorting all of them.
func oracleTopN(hits []sortedHit, n int, rank sortRanking) []sortedHit {
	all := append([]sortedHit(nil), hits...)
	sort.Slice(all, func(i, j int) bool { return rank.compare(&all[i], &all[j]) < 0 })
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// The top n of a stream of hits is what sorting them all gives, whatever the sort
// values (all alike, or all different), the order of the sort (a mix of ascending,
// descending and by score), the number of hits (fewer than n, a lot more), and
// however often the buffer is truncated on the way.
func TestSortedTopNIsTheBestNOfEverythingOffered(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	rankings := []sortRanking{
		{{key: 0}},
		{{key: 0, desc: true}},
		{{score: true, desc: true}},
		{{score: true}},
		{{key: 0, desc: true}, {score: true}},
		{{score: true, desc: true}, {key: 0}, {key: 1, desc: true}},
	}
	for ri, rank := range rankings {
		nkeys := 0
		for _, c := range rank {
			if !c.score {
				nkeys = max(nkeys, c.key+1)
			}
		}
		for _, alphabet := range []int{1, 2, 5, 200} {
			for _, count := range []int{0, 1, 7, 100, 3000} {
				for _, n := range []int{1, 2, 3, 10, 100, 5000} {
					t.Run(fmt.Sprintf("rank%d/alphabet%d/hits%d/n%d", ri, alphabet, count, n), func(t *testing.T) {
						hits := randomHits(rnd, count, nkeys, alphabet)
						top := newSortedTopN(n, rank)
						for i := range hits {
							if top.beats(&hits[i]) {
								top.add(hits[i])
							}
						}
						got := top.sorted()
						want := oracleTopN(hits, n, rank)
						if len(got) != len(want) {
							t.Fatalf("%d hits, want %d", len(got), len(want))
						}
						for i := range want {
							if got[i].ord != want[i].ord {
								t.Fatalf("hit %d is the %dth found, want the %dth", i, got[i].ord, want[i].ord)
							}
						}
					})
				}
			}
		}
	}
}

// A hit that ties the threshold on every sort value ranks below it, as it was
// found later: it has to be turned down, or the top would fill with later hits at
// the expense of earlier ones.
func TestSortedTopNTurnsDownATieOfTheThreshold(t *testing.T) {
	rank := sortRanking{{key: 0}}
	top := newSortedTopN(2, rank)
	for i := 0; i < 10; i++ {
		h := sortedHit{ord: uint64(i + 1), doc: uint64(i), keys: [][]byte{{'a'}}}
		if top.beats(&h) {
			top.add(h)
		}
	}
	got := top.sorted()
	if len(got) != 2 || got[0].ord != 1 || got[1].ord != 2 {
		t.Fatalf("kept %+v, want the two first hits found", got)
	}
}

// Quickselect has to work on whatever order the hits come in, sorted or reversed
// included, which is the case where bad pivots are likely.
func TestSelectNthOnSortedAndReversedInput(t *testing.T) {
	rank := sortRanking{{key: 0}}
	for _, reversed := range []bool{false, true} {
		const count = 20000
		hits := make([]sortedHit, count)
		for i := range hits {
			v := i
			if reversed {
				v = count - 1 - i
			}
			hits[i] = sortedHit{ord: uint64(i + 1), keys: [][]byte{{byte(v >> 8), byte(v)}}}
		}
		n := 1234
		selectNth(hits, n, rank)
		// everything before n ranks before the n-th, everything after ranks after
		for i := 0; i < count; i++ {
			c := rank.compare(&hits[i], &hits[n])
			if (i < n && c >= 0) || (i > n && c <= 0) {
				t.Fatalf("reversed=%v: position %d is out of place (%d)", reversed, i, c)
			}
		}
	}
}
