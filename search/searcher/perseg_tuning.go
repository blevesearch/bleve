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

import "sync/atomic"

// Tuning of the per segment algorithms: where the choice between two algorithms
// that give the same answer is one to make, these are what tests and benchmarks
// use to force it. Like DisjunctionHeapTakeover they are for whoever wants to
// compare algorithms; the defaults are right for searching. They are atomics, so
// changing one while searches are running is safe -- it takes effect for the
// segments that start after.

const (
	disjunctionAlgoAuto int32 = iota
	disjunctionAlgoWAND
	disjunctionAlgoMaxScore
)

// disjunctionAlgo is the algorithm that finds the best hits of a plain OR of terms
var disjunctionAlgo atomic.Int32

// conjBitmapMinCandidates is how many of the leader's postings in a window of an AND
// have to be able to make the heap for the window to be done on bitmaps.
var conjBitmapMinCandidates atomic.Int32

func init() {
	conjBitmapMinCandidates.Store(16)
}

// SetPerSegmentDisjunctionAlgo makes the searchers of a plain OR of terms use the
// algorithm named ("wand", "maxscore", or anything else for what fits best), and
// returns a function that puts back what was.
func SetPerSegmentDisjunctionAlgo(algo string) (restore func()) {
	next := disjunctionAlgoAuto
	switch algo {
	case "wand":
		next = disjunctionAlgoWAND
	case "maxscore":
		next = disjunctionAlgoMaxScore
	}
	prev := disjunctionAlgo.Swap(next)
	return func() { disjunctionAlgo.Store(prev) }
}

// SetPerSegmentConjunctionBitmapMinCandidates sets how many of the leader's postings
// in a window of an AND have to be able to make the heap for the window to be done
// on bitmaps (a very large number turns that off), and returns a function that puts
// back what was.
func SetPerSegmentConjunctionBitmapMinCandidates(n int) (restore func()) {
	prev := conjBitmapMinCandidates.Swap(int32(min(n, 1<<30)))
	return func() { conjBitmapMinCandidates.Store(prev) }
}
