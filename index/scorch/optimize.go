//  Copyright (c) 2018 Couchbase, Inc.
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

package scorch

import (
	"fmt"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var OptimizeConjunction = true
var OptimizeConjunctionUnadorned = true
var OptimizeDisjunctionUnadorned = true

func (s *IndexSnapshotTermFieldReader) Optimize(kind string,
	octx index.OptimizableContext) (index.OptimizableContext, error) {
	if OptimizeConjunction && kind == "conjunction" {
		return s.optimizeConjunction(octx)
	}

	if OptimizeConjunctionUnadorned && kind == "conjunction:unadorned" {
		return s.optimizeConjunctionUnadorned(octx)
	}

	if OptimizeDisjunctionUnadorned && kind == "disjunction:unadorned" {
		return s.optimizeDisjunctionUnadorned(octx)
	}

	return nil, nil
}

var OptimizeDisjunctionUnadornedMinChildCardinality = uint64(256)

// ----------------------------------------------------------------

func (s *IndexSnapshotTermFieldReader) optimizeConjunction(
	octx index.OptimizableContext) (index.OptimizableContext, error) {
	if octx == nil {
		octx = &OptimizeTFRConjunction{snapshot: s.snapshot}
	}

	o, ok := octx.(*OptimizeTFRConjunction)
	if !ok {
		return octx, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize conjunction across different snapshots")
	}

	o.tfrs = append(o.tfrs, s)

	return o, nil
}

type OptimizeTFRConjunction struct {
	snapshot *IndexSnapshot

	tfrs []*IndexSnapshotTermFieldReader
}

func (o *OptimizeTFRConjunction) Finish() (index.Optimized, error) {
	if len(o.tfrs) <= 1 {
		return nil, nil
	}

	for i := range o.snapshot.segment {
		// All clauses have to support being narrowed for this segment to be
		// worth touching at all -- ReplaceActual is what makes the narrowed
		// set stick for the scoring pass that runs after Finish returns.
		itrs := make([]segment.OptimizablePostingsIterator, len(o.tfrs))
		allOptimizable := true
		for ti, tfr := range o.tfrs {
			itr, ok := tfr.iterators[i].(segment.OptimizablePostingsIterator)
			if !ok {
				allOptimizable = false
				break
			}
			itrs[ti] = itr
		}
		if !allOptimizable {
			continue
		}

		bm, err := intersectPostingsForSegment(o.tfrs, itrs, i)
		if err != nil {
			return nil, err
		}
		if bm == nil {
			continue
		}

		// in this conjunction optimization, the postings iterators
		// will all share the same intersected actual bitmap.  The
		// regular conjunction searcher machinery will still be used,
		// but the underlying bitmap will be smaller.
		for _, itr := range itrs {
			itr.ReplaceActual(bm)
		}
	}

	return nil, nil
}

// leapfrogOverheadFactor governs the choice between the two ways of
// computing a conjunction's intersection, below. Materializing touches
// sum(counts) documents; leapfrogging touches roughly minCount*(K-1). Those
// two estimates alone say leapfrog should win whenever clauses aren't
// perfectly equal in size -- but they cost different amounts *per document
// touched*: materializing decodes whole blocks in a tight sequential SIMD
// loop, while leapfrogging pays a published-interface Advance() call (virtual
// dispatch, general seek bookkeeping meant to serve every caller of
// PostingsIterator, not just this one) for every probe. Measured on roughly
// equal-sized clauses -- where the two document-count estimates come out
// equal and so say "doesn't matter either way" -- leapfrog measured ~40%
// slower net of that gap, and on two ~140k-document clauses (a case the raw
// estimates would also call "close") it was slower still. This factor is a
// blunt way of saying "only leapfrog when the size skew clearly outweighs its
// higher per-document cost," not a claim that the ratio is exactly 4 -- it
// hasn't been tuned finely, just enough to stop misclassifying the two
// measured near-equal-size cases while still firing on the 127x-skewed one.
const leapfrogOverheadFactor = 4

// intersectPostingsForSegment computes the exact set of documents that match
// every one of tfrs' postings lists in segment segIdx, picking whichever of
// two strategies is cheaper for this particular set of clause sizes.
//
// Returns a nil bitmap (not an error) when nothing should change for this
// segment -- the caller then leaves its iterators alone rather than
// replacing them with an equivalent-but-freshly-built set.
func intersectPostingsForSegment(tfrs []*IndexSnapshotTermFieldReader,
	itrs []segment.OptimizablePostingsIterator, segIdx int) (*roaring.Bitmap, error) {
	var sumCounts, minCount uint64
	for i, tfr := range tfrs {
		pl := tfr.postings[segIdx]
		if pl == nil {
			return roaring.New(), nil // a clause has nothing in this segment
		}
		// NOTE: Count() is O(1) for a segment with no deletions on this
		// term, but can cost as much as a full decode when there are --
		// see zapx's PostingsList.Count(). That's an existing, separate gap:
		// a docFreq upper bound would suffice here and never need one, but
		// no such estimator is in the published segment API today.
		c := pl.Count()
		sumCounts += c
		if i == 0 || c < minCount {
			minCount = c
		}
	}

	if minCount*uint64(len(tfrs)-1)*leapfrogOverheadFactor < sumCounts {
		return leapfrogIntersect(tfrs, segIdx, minCount)
	}
	return materializeIntersect(itrs)
}

// materializeIntersect is the original strategy: ask every clause for its
// full doc set and roaring.And them together. Cheapest when clauses are
// comparably sized, because a bulk block decode beats leapfrog's per-document
// overhead by more than the two ever differ in document count.
func materializeIntersect(itrs []segment.OptimizablePostingsIterator) (*roaring.Bitmap, error) {
	bm0 := itrs[0].ActualBitmap()
	if bm0 == nil {
		return nil, nil
	}
	bm1 := itrs[1].ActualBitmap()
	if bm1 == nil {
		return nil, nil
	}
	bm := roaring.And(bm0, bm1)

	for _, itr := range itrs[2:] {
		bmN := itr.ActualBitmap()
		if bmN == nil {
			return nil, nil
		}
		bm.And(bmN)
	}

	return bm, nil
}

// leapfrogIntersect is a zig-zag merge driven directly by each clause's own
// Advance(), for when one clause is far smaller than the rest. Materializing
// every clause first -- what used to be free when a segment's on-disk
// postings representation already *was* a roaring bitmap sitting in memory --
// costs a full decode of the *largest* clause under a format under no
// obligation to keep one of those around (zapx v18's bitpacked blocks, for
// one). Decoding a 140,000-document postings list just to discover it
// intersects a 1,000-document one down to 934 hits is exactly the waste this
// avoids: cost here is bounded by the smallest clause, not the largest.
//
// Only Next()/Advance() from the published segment.PostingsIterator interface
// are used, so this helps every segment implementation, not only ones with an
// expensive ActualBitmap().
func leapfrogIntersect(tfrs []*IndexSnapshotTermFieldReader, segIdx int,
	minCount uint64) (*roaring.Bitmap, error) {
	itrs := make([]segment.PostingsIterator, len(tfrs))
	for i, tfr := range tfrs {
		// A throwaway, freq/norm/loc-free iterator: narrowing only needs doc
		// numbers. Reusing tfr.iterators[segIdx] here would consume the
		// position the real scoring pass still needs to start from -- this
		// runs before any scoring has happened, and must leave those
		// iterators exactly as it found them.
		itrs[i] = tfr.postings[segIdx].Iterator(false, false, false, nil)
	}

	cur := make([]uint64, len(itrs))
	for i, itr := range itrs {
		p, err := itr.Next()
		if err != nil {
			return nil, err
		}
		if p == nil {
			return roaring.New(), nil // a clause is empty in this segment
		}
		cur[i] = p.Number()
	}

	// The intersection can never exceed the smallest clause, so its count
	// bounds how large `matched` will grow -- without this, appending a
	// large result one element at a time re-grows and re-copies the slice
	// through every capacity doubling, which can dwarf the cost of the merge
	// itself.
	matched := make([]uint32, 0, minCount)
	for {
		var maxDoc uint64
		for _, c := range cur {
			if c > maxDoc {
				maxDoc = c
			}
		}

		allEqual := true
		for i, itr := range itrs {
			if cur[i] < maxDoc {
				p, err := itr.Advance(maxDoc)
				if err != nil {
					return nil, err
				}
				if p == nil {
					bm := roaring.New()
					bm.AddMany(matched)
					return bm, nil
				}
				cur[i] = p.Number()
			}
			if cur[i] != maxDoc {
				allEqual = false
			}
		}
		if !allEqual {
			continue
		}

		matched = append(matched, uint32(maxDoc))

		exhausted := false
		for i, itr := range itrs {
			p, err := itr.Advance(maxDoc + 1)
			if err != nil {
				return nil, err
			}
			if p == nil {
				exhausted = true
				break
			}
			cur[i] = p.Number()
		}
		if exhausted {
			break
		}
	}

	bm := roaring.New()
	bm.AddMany(matched)
	return bm, nil
}

// ----------------------------------------------------------------

// An "unadorned" conjunction optimization is appropriate when
// additional or subsidiary information like freq-norm's and
// term-vectors are not required, and instead only the internal-id's
// are needed.
func (s *IndexSnapshotTermFieldReader) optimizeConjunctionUnadorned(
	octx index.OptimizableContext) (index.OptimizableContext, error) {
	if octx == nil {
		octx = &OptimizeTFRConjunctionUnadorned{snapshot: s.snapshot}
	}

	o, ok := octx.(*OptimizeTFRConjunctionUnadorned)
	if !ok {
		return nil, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize unadorned conjunction across different snapshots")
	}

	o.tfrs = append(o.tfrs, s)

	return o, nil
}

type OptimizeTFRConjunctionUnadorned struct {
	snapshot *IndexSnapshot

	tfrs []*IndexSnapshotTermFieldReader
}

var OptimizeTFRConjunctionUnadornedTerm = []byte("<conjunction:unadorned>")
var OptimizeTFRConjunctionUnadornedField = "*"

// Finish of an unadorned conjunction optimization will compute a
// termFieldReader with an "actual" bitmap that represents the
// constituent bitmaps AND'ed together.  This termFieldReader cannot
// provide any freq-norm or termVector associated information.
func (o *OptimizeTFRConjunctionUnadorned) Finish() (rv index.Optimized, err error) {
	if len(o.tfrs) <= 1 {
		return nil, nil
	}

	// We use an artificial term and field because the optimized
	// termFieldReader can represent multiple terms and fields.
	oTFR := o.snapshot.unadornedTermFieldReader(
		OptimizeTFRConjunctionUnadornedTerm, OptimizeTFRConjunctionUnadornedField)

	var actualBMs []*roaring.Bitmap // Collected from regular posting lists.

OUTER:
	for i := range o.snapshot.segment {
		actualBMs = actualBMs[:0]

		var docNum1HitLast uint64
		var docNum1HitLastOk bool

		for _, tfr := range o.tfrs {
			if _, ok := tfr.iterators[i].(*emptyPostingsIterator); ok {
				// An empty postings iterator means the entire AND is empty.
				oTFR.iterators[i] = anEmptyPostingsIterator
				continue OUTER
			}

			itr, ok := tfr.iterators[i].(segment.OptimizablePostingsIterator)
			if !ok {
				// We only optimize postings iterators that support this operation.
				return nil, nil
			}

			// If the postings iterator is "1-hit" optimized, then we
			// can perform several optimizations up-front here.
			docNum1Hit, ok := itr.DocNum1Hit()
			if ok {
				if docNum1HitLastOk && docNum1HitLast != docNum1Hit {
					// The docNum1Hit doesn't match the previous
					// docNum1HitLast, so the entire AND is empty.
					oTFR.iterators[i] = anEmptyPostingsIterator
					continue OUTER
				}

				docNum1HitLast = docNum1Hit
				docNum1HitLastOk = true

				continue
			}

			if itr.ActualBitmap() == nil {
				// An empty actual bitmap means the entire AND is empty.
				oTFR.iterators[i] = anEmptyPostingsIterator
				continue OUTER
			}

			// Collect the actual bitmap for more processing later.
			actualBMs = append(actualBMs, itr.ActualBitmap())
		}

		if docNum1HitLastOk {
			// We reach here if all the 1-hit optimized posting
			// iterators had the same 1-hit docNum, so we can check if
			// our collected actual bitmaps also have that docNum.
			for _, bm := range actualBMs {
				if !bm.Contains(uint32(docNum1HitLast)) {
					// The docNum1Hit isn't in one of our actual
					// bitmaps, so the entire AND is empty.
					oTFR.iterators[i] = anEmptyPostingsIterator
					continue OUTER
				}
			}

			// The actual bitmaps and docNum1Hits all contain or have
			// the same 1-hit docNum, so that's our AND'ed result.
			oTFR.iterators[i] = newUnadornedPostingsIteratorFrom1Hit(docNum1HitLast)

			continue OUTER
		}

		if len(actualBMs) == 0 {
			// If we've collected no actual bitmaps at this point,
			// then the entire AND is empty.
			oTFR.iterators[i] = anEmptyPostingsIterator
			continue OUTER
		}

		if len(actualBMs) == 1 {
			// If we've only 1 actual bitmap, then that's our result.
			oTFR.iterators[i] = newUnadornedPostingsIteratorFromBitmap(actualBMs[0])

			continue OUTER
		}

		// Else, AND together our collected bitmaps as our result.
		bm := roaring.And(actualBMs[0], actualBMs[1])

		for _, actualBM := range actualBMs[2:] {
			bm.And(actualBM)
		}

		oTFR.iterators[i] = newUnadornedPostingsIteratorFromBitmap(bm)
	}

	atomic.AddUint64(&o.snapshot.parent.stats.TotTermSearchersStarted, uint64(1))
	return oTFR, nil
}

// ----------------------------------------------------------------

// An "unadorned" disjunction optimization is appropriate when
// additional or subsidiary information like freq-norm's and
// term-vectors are not required, and instead only the internal-id's
// are needed.
func (s *IndexSnapshotTermFieldReader) optimizeDisjunctionUnadorned(
	octx index.OptimizableContext) (index.OptimizableContext, error) {
	if octx == nil {
		octx = &OptimizeTFRDisjunctionUnadorned{
			snapshot: s.snapshot,
		}
	}

	o, ok := octx.(*OptimizeTFRDisjunctionUnadorned)
	if !ok {
		return nil, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize unadorned disjunction across different snapshots")
	}

	o.tfrs = append(o.tfrs, s)

	return o, nil
}

type OptimizeTFRDisjunctionUnadorned struct {
	snapshot *IndexSnapshot

	tfrs []*IndexSnapshotTermFieldReader
}

var OptimizeTFRDisjunctionUnadornedTerm = []byte("<disjunction:unadorned>")
var OptimizeTFRDisjunctionUnadornedField = "*"

// Finish of an unadorned disjunction optimization will compute a
// termFieldReader with an "actual" bitmap that represents the
// constituent bitmaps OR'ed together.  This termFieldReader cannot
// provide any freq-norm or termVector associated information.
func (o *OptimizeTFRDisjunctionUnadorned) Finish() (rv index.Optimized, err error) {
	if len(o.tfrs) <= 1 {
		return nil, nil
	}

	// We use an artificial term and field because the optimized
	// termFieldReader can represent multiple terms and fields.
	oTFR := o.snapshot.unadornedTermFieldReader(
		OptimizeTFRDisjunctionUnadornedTerm, OptimizeTFRDisjunctionUnadornedField)

	var docNums []uint32            // Collected docNum's from 1-hit posting lists.
	var actualBMs []*roaring.Bitmap // Collected from regular posting lists.

	for i := range o.snapshot.segment {
		docNums = docNums[:0]
		actualBMs = actualBMs[:0]

		for _, tfr := range o.tfrs {
			itr, ok := tfr.iterators[i].(segment.OptimizablePostingsIterator)
			if !ok {
				return nil, nil
			}

			docNum, ok := itr.DocNum1Hit()
			if ok {
				docNums = append(docNums, uint32(docNum))
				continue
			}

			if itr.ActualBitmap() != nil {
				actualBMs = append(actualBMs, itr.ActualBitmap())
			}
		}

		var bm *roaring.Bitmap
		if len(actualBMs) > 2 {
			bm = roaring.HeapOr(actualBMs...)
		} else if len(actualBMs) == 2 {
			bm = roaring.Or(actualBMs[0], actualBMs[1])
		} else if len(actualBMs) == 1 {
			bm = actualBMs[0].Clone()
		} else {
			if len(docNums) == 0 {
				// no hits, reuse the zero-alloc empty sentinel
				oTFR.iterators[i] = anEmptyPostingsIterator
				continue
			} else if len(docNums) == 1 {
				// 1-hit optimized
				oTFR.iterators[i] = newUnadornedPostingsIteratorFrom1Hit(uint64(docNums[0]))
				continue
			} else {
				bm = roaring.New()
			}
		}

		bm.AddMany(docNums)

		oTFR.iterators[i] = newUnadornedPostingsIteratorFromBitmap(bm)
	}

	atomic.AddUint64(&o.snapshot.parent.stats.TotTermSearchersStarted, uint64(1))
	return oTFR, nil
}

// ----------------------------------------------------------------

func (i *IndexSnapshot) unadornedTermFieldReader(
	term []byte, field string) *IndexSnapshotTermFieldReader {
	// This IndexSnapshotTermFieldReader will not be recycled, more
	// conversation here: https://github.com/blevesearch/bleve/pull/1438
	return &IndexSnapshotTermFieldReader{
		term:               term,
		field:              field,
		snapshot:           i,
		iterators:          make([]segment.PostingsIterator, len(i.segment)),
		segmentOffset:      0,
		includeFreq:        false,
		includeNorm:        false,
		includeTermVectors: false,
		recycle:            false,
		// signal downstream that this is a special unadorned termFieldReader
		unadorned: true,
		// unadorned TFRs do not require bytes read tracking
		updateBytesRead: false,
	}
}
