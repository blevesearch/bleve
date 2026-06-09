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

type IndexSnapshotTermFieldReader struct {
	term               []byte
	field              string
	snapshot           *IndexSnapshot
	dicts              []segment.TermDictionary
	postings           []segment.PostingsList
	iterators          []segment.PostingsIterator
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
	if i.currPosting != nil && i.currID.Compare(ID) >= 0 {
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

// maxTFNormProvider is the optional interface implemented by zapx.Dictionary.
type maxTFNormProvider interface {
	MaxTFNorm(term []byte, avgDocLength float64) float32
}

// MaxTFNorm returns the max BM25 tf-norm contribution for this term across
// all segments, using the lazy per-segment cache in zapx.  Returns 0 if
// avgDocLength is 0 (TF-IDF mode) or the term is not found anywhere.
//
// Cost: O(N_segments) — each call does N RLock + map-lookup operations
// against the per-segment invertedCacheEntry.maxTFNormCache (see
// zapx/inverted_text_cache.go).  Those lookups are fast (warm cache ≈ 20ns
// each), but for N=15 segments and 3 query terms that is ~900ns per query.
//
// FUTURE: cache the cross-segment max at IndexSnapshot level so repeated
// queries (same or different clients) pay one lookup instead of N.
func (i *IndexSnapshotTermFieldReader) MaxTFNorm(avgDocLength float64) float32 {
	if avgDocLength <= 0 {
		return 0
	}
	var maxV float32
	for _, dict := range i.dicts {
		if p, ok := dict.(maxTFNormProvider); ok {
			if v := p.MaxTFNorm(i.term, avgDocLength); v > maxV {
				maxV = v
			}
		}
	}
	return maxV
}

// NumSegments returns the number of segments in the index snapshot.
func (i *IndexSnapshotTermFieldReader) NumSegments() int {
	return len(i.snapshot.segment)
}

// MaxTFNormForSegment returns the max BM25 tf-norm for this term in a specific
// segment. Returns 0 if the term is absent from that segment or avgDocLength≤0.
func (i *IndexSnapshotTermFieldReader) MaxTFNormForSegment(segIdx int, avgDocLength float64) float32 {
	if avgDocLength <= 0 || segIdx >= len(i.dicts) {
		return 0
	}
	if p, ok := i.dicts[segIdx].(maxTFNormProvider); ok {
		return p.MaxTFNorm(i.term, avgDocLength)
	}
	return 0
}

// SegmentIndexOf returns the segment index for the given global docID.
func (i *IndexSnapshotTermFieldReader) SegmentIndexOf(id index.IndexInternalID) int {
	num, err := id.Value()
	if err != nil {
		return 0
	}
	segIdx, _ := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(num)
	return segIdx
}

// FirstDocIDOfSegment returns the first global docID in segment segIdx, using
// buf for the backing storage. Returns nil if segIdx >= NumSegments().
func (i *IndexSnapshotTermFieldReader) FirstDocIDOfSegment(segIdx int, buf []byte) index.IndexInternalID {
	if segIdx >= len(i.snapshot.offsets) {
		return nil
	}
	return index.NewIndexInternalID(buf, i.snapshot.offsets[segIdx])
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
