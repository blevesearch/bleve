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

// blockFiller is the bulk form of the same idea: hand back a whole block of
// postings as flat arrays so the caller can score them without a per-document
// object. Implementations do not decode locations.
type blockFiller interface {
	NextBlock(docNums []uint64, freqs []uint64, norms []float64,
		globalOffset uint64) (int, error)
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
		bf, _ := i.iterators[i.segmentOffset].(blockFiller)
		if bf == nil {
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
		if _, ok := it.(blockFiller); !ok {
			return false
		}
	}
	return true
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
	recycle            bool
	bytesRead          uint64
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
	num := ID.Value()
	segIndex, ldocNum := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(num)
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
		if preAlloced == nil {
			preAlloced = &index.TermFieldDoc{}
		}
		found, err := filler.FillTermFieldDoc(preAlloced, i.snapshot.offsets[segIndex],
			ldocNum, i.includeFreq, i.includeNorm)
		if err != nil {
			return nil, err
		}
		if !found {
			// nothing at or after the target in this segment; Next picks up
			// from the following segment (segmentOffset already moved)
			return i.Next(preAlloced)
		}
		i.currID = preAlloced.ID
		i.currPosting = nil
		return preAlloced, nil
	}

	next, err := i.iterators[i.segmentOffset].Advance(ldocNum)
	if err != nil {
		return nil, err
	}
	if next == nil {
		// we jumped directly to the segment that should have contained it
		// but it wasn't there, so reuse Next() which should correctly
		// get the next hit after it (we moved i.segmentOffset)
		return i.Next(preAlloced)
	}

	if preAlloced == nil {
		preAlloced = &index.TermFieldDoc{}
	}
	preAlloced.ID = index.NewIndexInternalID(preAlloced.ID, next.Number()+
		i.snapshot.offsets[segIndex])
	i.postingToTermFieldDoc(next, preAlloced)
	i.currID = preAlloced.ID
	i.currPosting = next
	return preAlloced, nil
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
