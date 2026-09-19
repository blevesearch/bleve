//  Copyright (c) 2017 Couchbase, Inc.
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
	"context"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizeIndexSnapshotTermFieldReader int

func init() {
	var istfr IndexSnapshotTermFieldReader
	reflectStaticSizeIndexSnapshotTermFieldReader = int(reflect.TypeOf(istfr).Size())
}

// termFieldDocFiller is an optional fast path a segment's postings iterator may
// implement: fill a TermFieldDoc directly instead of returning a boxed
// segment.Posting that the caller then interrogates via Number/Frequency/Norm.
// Those four virtual calls plus the Posting struct clear measured at roughly a
// quarter of the term-scan inner loop.
//
// It is only usable when term vectors are not requested — implementations are
// not required to decode locations.
type termFieldDocFiller interface {
	FillTermFieldDoc(rv *index.TermFieldDoc, globalOffset, atOrAfter uint64,
		includeFreq, includeNorm bool) (bool, error)
}

// NextBlock fills the caller's arrays with the next block of postings, walking
// across segments as needed. It returns 0 when the reader is exhausted.
//
// This exists so a searcher can pull postings in bulk instead of one
// TermFieldDoc at a time; profiling put roughly half of a term scan in the
// per-document plumbing between here and the collector.
func (i *IndexSnapshotTermFieldReader) NextBlock(docNums []uint64, freqs []uint64,
	norms []float64) (int, error) {
	n := 0
	for n < len(docNums) && i.segmentOffset < len(i.iterators) {
		// asserted here rather than precomputed per reader: multi-term queries
		// build hundreds of readers and would pay for a capability they never
		// use, while here the cost is amortised over a whole block
		bf, ok := i.iterators[i.segmentOffset].(segment.BlockMaxPostingsIterator)
		if !ok {
			return n, nil // segment can't bulk-fill; caller falls back
		}
		curItr := i.iterators[i.segmentOffset]
		var prevBytesRead uint64
		if i.updateBytesRead {
			prevBytesRead = curItr.BytesRead()
		}
		got, err := bf.NextBlock(docNums[n:], freqs[n:], norms[n:],
			i.snapshot.offsets[i.segmentOffset])
		if err != nil {
			return n, err
		}
		if i.updateBytesRead {
			if bytesRead := curItr.BytesRead(); bytesRead > prevBytesRead {
				i.incrementBytesRead(bytesRead - prevBytesRead)
			}
		}
		n += got
		if got == 0 || n == len(docNums) {
			if got == 0 {
				i.segmentOffset++
				continue
			}
			break
		}
		// the segment is drained but there is room left in the block
		i.segmentOffset++
	}
	if n > 0 {
		i.currID = index.NewIndexInternalID(i.currID, docNums[n-1])
		i.currPosting = nil
	}
	return n, nil
}

// supportsBlocks reports whether every segment can bulk-fill, which is what
// lets a caller commit to the block path for the whole reader.
func (i *IndexSnapshotTermFieldReader) SupportsBlocks() bool {
	if i.includeTermVectors || len(i.iterators) == 0 {
		return false
	}
	for _, it := range i.iterators {
		if _, ok := it.(segment.BlockMaxPostingsIterator); !ok {
			return false
		}
	}
	return true
}

// BlockMax reports segment.BlockMaxPostingsIterator's bound for whichever
// segment this reader is currently positioned at, translated to a global
// document number. docCount is the number of documents that span covers --
// letting a caller that skips it keep an exact hit count without knowing
// anything about the segment's block size. ok is false whenever there's
// nothing useful to report -- the reader is exhausted, or the current
// segment's iterator doesn't support it (a 1-hit term, a conjunction-narrowed
// iterator, live deletions, or a segment implementation that simply doesn't
// have this capability) -- in which case the caller should just proceed with
// a normal fetch.
func (i *IndexSnapshotTermFieldReader) BlockMax() (maxTF uint64, maxNormFactor float64, lastDoc uint64, docCount int, ok bool) {
	if i.segmentOffset >= len(i.iterators) {
		return 0, 0, 0, 0, false
	}
	bm, ok := i.iterators[i.segmentOffset].(segment.BlockMaxPostingsIterator)
	if !ok {
		return 0, 0, 0, 0, false
	}
	maxTF, maxNormFactor, segLastDoc, docCount, ok := bm.BlockMax()
	if !ok {
		return 0, 0, 0, 0, false
	}
	return maxTF, maxNormFactor, segLastDoc + i.snapshot.offsets[i.segmentOffset], docCount, true
}

// ShallowAdvance moves this reader to the block that could contain the given
// global document number, without decoding any payload. Call BlockMax again
// afterward for the new position's bound before deciding whether to fetch it
// for real.
//
// Like Advance, target must be strictly greater than any document number this
// reader has already produced or shallow-advanced past. Unlike Advance, there
// is no backward-seek recovery: ShallowAdvance is meant to be driven by a
// BlockMax bound this same reader just reported, which is inherently
// forward-only, so callers don't need it.
func (i *IndexSnapshotTermFieldReader) ShallowAdvance(target uint64) error {
	segIndex, ldocNum := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(target)
	if segIndex >= len(i.snapshot.segment) {
		i.segmentOffset = len(i.iterators)
		return nil
	}
	i.segmentOffset = segIndex
	if sa, ok := i.iterators[i.segmentOffset].(segment.BlockMaxPostingsIterator); ok {
		return sa.ShallowAdvance(ldocNum)
	}
	return nil
}

type IndexSnapshotTermFieldReader struct {
	term      []byte
	field     string
	snapshot  *IndexSnapshot
	dicts     []segment.TermDictionary
	postings  []segment.PostingsList
	iterators []segment.PostingsIterator
	// fillers[i] is iterators[i] if it supports the direct-fill fast path and
	// that path is applicable, else nil. Resolved once per reader so the hot
	// loop doesn't repeat the type assertion.
	fillers            []termFieldDocFiller
	segmentOffset      int
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
	currPosting        segment.Posting
	currID             index.IndexInternalID
	// advanceScratch is AdvanceDocNum's private scratch TermFieldDoc: it
	// never leaves the method as a pointer, only as the raw values a caller
	// asked for, so reusing one instance across calls is safe the same way
	// currID's own buffer reuse already is.
	advanceScratch index.TermFieldDoc
	recycle        bool
	bytesRead      uint64
	ctx                context.Context
	unadorned          bool
	// flag to indicate whether to increment our bytesRead
	// value after creation of the TFR while iterating our postings
	// lists
	updateBytesRead bool
}

func (i *IndexSnapshotTermFieldReader) incrementBytesRead(val uint64) {
	i.bytesRead += val
}

func (i *IndexSnapshotTermFieldReader) Size() int {
	sizeInBytes := reflectStaticSizeIndexSnapshotTermFieldReader + size.SizeOfPtr +
		len(i.term) +
		len(i.field) +
		len(i.currID)

	for _, entry := range i.postings {
		sizeInBytes += entry.Size()
	}

	for _, entry := range i.iterators {
		sizeInBytes += entry.Size()
	}

	if i.currPosting != nil {
		sizeInBytes += i.currPosting.Size()
	}

	return sizeInBytes
}

func (i *IndexSnapshotTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.TermFieldDoc{}
	}
	var prevBytesRead uint64
	// find the next hit
	for i.segmentOffset < len(i.iterators) {
		// get our current postings iterator
		curItr := i.iterators[i.segmentOffset]
		if i.updateBytesRead {
			prevBytesRead = curItr.BytesRead()
		}

		// Fast path: let the segment write straight into rv, skipping the
		// per-document Posting boxing and its accessor calls.
		//
		// fillers is bounds-checked rather than indexed directly: the unadorned
		// and optimized readers build a TFR without it, so it may be shorter
		// than iterators (or nil).
		var filler termFieldDocFiller
		if i.segmentOffset < len(i.fillers) {
			filler = i.fillers[i.segmentOffset]
		}
		if filler != nil {
			globalOffset := i.snapshot.offsets[i.segmentOffset]
			found, err := filler.FillTermFieldDoc(rv, globalOffset, 0, i.includeFreq, i.includeNorm)
			if err != nil {
				return nil, err
			}
			if found {
				i.currID = rv.ID
				i.currPosting = nil // the fast path produces no Posting
				if i.updateBytesRead {
					if bytesRead := curItr.BytesRead(); bytesRead > prevBytesRead {
						i.incrementBytesRead(bytesRead - prevBytesRead)
					}
				}
				return rv, nil
			}
			i.segmentOffset++
			continue
		}

		next, err := curItr.Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			globalOffset := i.snapshot.offsets[i.segmentOffset]
			nnum := next.Number()
			rv.ID = index.NewIndexInternalID(rv.ID, nnum+globalOffset)
			i.postingToTermFieldDoc(next, rv)

			i.currID = rv.ID
			i.currPosting = next
			if i.updateBytesRead {
				// postingsIterators maintains the bytesRead stat in a cumulative fashion.
				// this is because there are chances of having a series of loadChunk calls,
				// and they have to be added together before sending the bytesRead at this point
				// upstream.
				bytesRead := curItr.BytesRead()
				if bytesRead > prevBytesRead {
					i.incrementBytesRead(bytesRead - prevBytesRead)
				}
			}
			return rv, nil
		}
		i.segmentOffset++
	}
	return nil, nil
}

func (i *IndexSnapshotTermFieldReader) postingToTermFieldDoc(next segment.Posting, rv *index.TermFieldDoc) {
	if i.includeFreq {
		rv.Freq = next.Frequency()
	}
	if i.includeNorm {
		rv.Norm = next.Norm()
	}
	if i.includeTermVectors {
		locs := next.Locations()
		if cap(rv.Vectors) < len(locs) {
			rv.Vectors = make([]*index.TermFieldVector, len(locs))
			backing := make([]index.TermFieldVector, len(locs))
			for i := range backing {
				rv.Vectors[i] = &backing[i]
			}
		}
		rv.Vectors = rv.Vectors[:len(locs)]
		for i, loc := range locs {
			*rv.Vectors[i] = index.TermFieldVector{
				Start:          loc.Start(),
				End:            loc.End(),
				Pos:            loc.Pos(),
				ArrayPositions: loc.ArrayPositions(),
				Field:          loc.Field(),
			}
		}
	}
}

func (i *IndexSnapshotTermFieldReader) Advance(ID index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	// FIXME do something better
	// for now, if we need to seek backwards, then restart from the beginning
	// currID is non-empty exactly when this reader has already returned a hit.
	// currPosting cannot be used for that test: the direct-fill fast path in
	// Next never populates it.
	if len(i.currID) > 0 && i.currID.Compare(ID) >= 0 {
		// Check if the TFR is a special unadorned composite optimization.
		// Such a TFR will NOT have a valid `term` or `field` set, making it
		// impossible for the TFR to replace itself with a new one.
		if !i.unadorned {
			i2, err := i.snapshot.TermFieldReader(context.TODO(), i.term, i.field,
				i.includeFreq, i.includeNorm, i.includeTermVectors)
			if err != nil {
				return nil, err
			}
			// close the current term field reader before replacing it with a new one
			_ = i.Close()
			*i = *(i2.(*IndexSnapshotTermFieldReader))
		} else {
			// unadorned composite optimization
			// we need to reset all the iterators
			// back to the beginning, which effectively
			// achieves the same thing as the above
			for _, iter := range i.iterators {
				if optimizedIterator, ok := iter.(ResetablePostingsIterator); ok {
					optimizedIterator.ResetIterator()
				}
			}
		}
	}
	if preAlloced == nil {
		preAlloced = &index.TermFieldDoc{}
	}
	return i.advanceNum(ID.Value(), preAlloced)
}

// advanceNum is Advance's core logic, taking the target as a raw uint64
// rather than an encoded index.IndexInternalID -- shared by Advance (which
// decodes ID into this form before calling in) and AdvanceDocNum (which
// deals in raw document numbers to begin with, so has nothing to decode).
//
// Does not implement Advance's backward-seek recovery (rebuilding the whole
// reader when the target is at or before whatever this reader last
// produced): that check needs the actual index.IndexInternalID, which
// AdvanceDocNum's caller does not have and does not need, since it
// guarantees forward-only targets some other way. Advance performs that
// check itself, before calling in here.
func (i *IndexSnapshotTermFieldReader) advanceNum(target uint64, rv *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	segIndex, ldocNum := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(target)
	if segIndex >= len(i.snapshot.segment) {
		return nil, fmt.Errorf("computed segment index %d out of bounds %d",
			segIndex, len(i.snapshot.segment))
	}
	// skip directly to the target segment
	i.segmentOffset = segIndex

	// Same direct-fill fast path as Next, which matters here because
	// conjunctions drive their non-leading clauses entirely through Advance.
	var filler termFieldDocFiller
	if i.segmentOffset < len(i.fillers) {
		filler = i.fillers[i.segmentOffset]
	}
	if filler != nil {
		found, err := filler.FillTermFieldDoc(rv, i.snapshot.offsets[segIndex],
			ldocNum, i.includeFreq, i.includeNorm)
		if err != nil {
			return nil, err
		}
		if !found {
			// nothing at or after the target in this segment; Next picks up
			// from the following segment (segmentOffset already moved)
			return i.Next(rv)
		}
		i.currID = rv.ID
		i.currPosting = nil
		return rv, nil
	}

	next, err := i.iterators[i.segmentOffset].Advance(ldocNum)
	if err != nil {
		return nil, err
	}
	if next == nil {
		// we jumped directly to the segment that should have contained it
		// but it wasn't there, so reuse Next() which should correctly
		// get the next hit after it (we moved i.segmentOffset)
		return i.Next(rv)
	}

	rv.ID = index.NewIndexInternalID(rv.ID, next.Number()+i.snapshot.offsets[segIndex])
	i.postingToTermFieldDoc(next, rv)
	i.currID = rv.ID
	i.currPosting = next
	return rv, nil
}

// AdvanceDocNum is Advance's counterpart for a caller that already tracks
// document numbers as plain uint64s and has no other use for the generic
// index.IndexInternalID encoding -- block-conjunction WAND's per-candidate
// secondary membership check (search_conjunction_block.go's
// blockConjunction.scoreCandidates) is the motivating case. Encoding a
// target into ID bytes only for Advance to immediately decode it straight
// back via ID.Value(), then decoding the *index.TermFieldDoc it returns back
// into a uint64 again, is pure round-trip cost paid on every single
// candidate x secondary pair -- this skips both encode and both decodes,
// keeping only the one encode this reader's own currID bookkeeping always
// needs regardless of which entry point is used.
//
// Like Advance, target must be strictly greater than any document number
// this reader has already produced or advanced past. Advance recovers from
// a backward seek by rebuilding the reader from scratch; this method does
// not attempt to detect one at all, since block-conjunction WAND already
// guarantees forward-only targets by construction (candidates are visited
// in ascending doc order) and paying for that check here would defeat the
// point of avoiding the encode it needs in the first place.
func (i *IndexSnapshotTermFieldReader) AdvanceDocNum(target uint64) (docNum uint64, freq uint64, norm float64, exists bool, err error) {
	rv, err := i.advanceNum(target, &i.advanceScratch)
	if err != nil || rv == nil {
		return 0, 0, 0, false, err
	}
	return rv.ID.Value(), rv.Freq, rv.Norm, true, nil
}

func (i *IndexSnapshotTermFieldReader) Count() uint64 {
	var rv uint64
	for _, posting := range i.postings {
		rv += posting.Count()
	}
	return rv
}

func (i *IndexSnapshotTermFieldReader) Close() error {
	if i.ctx != nil {
		statsCallbackFn := i.ctx.Value(search.SearchIOStatsCallbackKey)
		if statsCallbackFn != nil {
			// essentially before you close the TFR, you must report this
			// reader's bytesRead value
			statsCallbackFn.(search.SearchIOStatsCallbackFunc)(i.bytesRead)
		}

		search.RecordSearchCost(i.ctx, search.AddM, i.bytesRead)
	}

	if i.snapshot != nil {
		atomic.AddUint64(&i.snapshot.parent.stats.TotTermSearchersFinished, uint64(1))
		i.snapshot.recycleTermFieldReader(i)
	}
	return nil
}
