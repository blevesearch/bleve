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
	// segmentBase is non-zero for shard TFRs created by TermFieldReaderForSegmentRange
	// (§7 parallel segment search). A shard TFR covers only snapshot.segment[segmentBase:
	// segmentBase+len(iterators)]; segmentOffset is relative to this range.
	// For normal full-index TFRs segmentBase == 0 and len(iterators) == len(snapshot.segment).
	segmentBase        int
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

	// segMaxTFNorms caches per-segment maxTFNorm values populated by MaxTFNorm().
	// MaxTFNormForSegment() uses this to avoid redundant invIndexCache lookups
	// when initWANDMaxImpacts calls both MaxTFNorm (once) and MaxTFNormForSegment
	// (×numSegments) per term searcher.
	segMaxTFNorms     []float32
	segMaxTFNormsAvgDl float64
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
			globalOffset := i.snapshot.offsets[i.segmentBase+i.segmentOffset]
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

// normByteProvider is the optional interface implemented by zapx.Posting
// to expose the raw SmallFloat norm byte from the norm column (§20/§25).
type normByteProvider interface {
	NormByte() uint8
}

func (i *IndexSnapshotTermFieldReader) postingToTermFieldDoc(next segment.Posting, rv *index.TermFieldDoc) {
	if i.includeFreq {
		rv.Freq = next.Frequency()
	}
	if i.includeNorm {
		rv.Norm = next.Norm()
		if nb, ok := next.(normByteProvider); ok {
			rv.NormByte = nb.NormByte()
		}
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
			// For shard TFRs (§7 parallel segment search), restart within the shard
			// range only so we don't escape the assigned segment group.
			isShardTFR := i.segmentBase > 0 || len(i.iterators) < len(i.snapshot.segment)
			var i2 index.TermFieldReader
			var err error
			if isShardTFR {
				endSeg := i.segmentBase + len(i.iterators)
				i2, err = i.snapshot.TermFieldReaderForSegmentRange(context.TODO(), i.term, i.field,
					i.includeFreq, i.includeNorm, i.includeTermVectors, i.segmentBase, endSeg)
			} else {
				i2, err = i.snapshot.TermFieldReader(context.TODO(), i.term, i.field,
					i.includeFreq, i.includeNorm, i.includeTermVectors)
			}
			if err != nil {
				return nil, err
			}
			i2tfr := i2.(*IndexSnapshotTermFieldReader)
			// Account for the current reader's lifecycle before we overwrite it.
			// We cannot call i.Close() here because the caller still holds i's
			// pointer — Close() would recycle i into the pool, making i available
			// to another goroutine while we then write *i = *i2tfr. That is a
			// data race (MB-64604). Instead, replicate the non-recycle parts of
			// Close() and skip recycleTermFieldReader.
			if i.ctx != nil {
				if fn := i.ctx.Value(search.SearchIOStatsCallbackKey); fn != nil {
					fn.(search.SearchIOStatsCallbackFunc)(i.bytesRead)
				}
				search.RecordSearchCost(i.ctx, search.AddM, i.bytesRead)
			}
			if i.snapshot != nil {
				atomic.AddUint64(&i.snapshot.parent.stats.TotTermSearchersFinished, uint64(1))
			}
			// Overwrite i in-place so the caller's pointer remains valid.
			// i2tfr is now an orphan; clear its recycle flag so it cannot be
			// added to the pool a second time if Close() is called on it.
			*i = *i2tfr
			i2tfr.recycle = false
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
	// For shard TFRs (§7): translate global segIndex to shard-relative offset.
	shardSegOffset := segIndex - i.segmentBase
	if shardSegOffset < 0 {
		// Target is before this shard; return the first match in the shard.
		i.segmentOffset = 0
		return i.Next(preAlloced)
	}
	if shardSegOffset >= len(i.iterators) {
		// Target is after this shard; shard is exhausted.
		i.segmentOffset = len(i.iterators)
		return nil, nil
	}
	// skip directly to the target segment
	i.segmentOffset = shardSegOffset
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
	// Populate per-segment cache for MaxTFNormForSegment reuse.
	if cap(i.segMaxTFNorms) < len(i.dicts) {
		i.segMaxTFNorms = make([]float32, len(i.dicts))
	} else {
		i.segMaxTFNorms = i.segMaxTFNorms[:len(i.dicts)]
	}
	i.segMaxTFNormsAvgDl = avgDocLength
	var maxV float32
	for j, dict := range i.dicts {
		var v float32
		if p, ok := dict.(maxTFNormProvider); ok {
			v = p.MaxTFNorm(i.term, avgDocLength)
		}
		i.segMaxTFNorms[j] = v
		if v > maxV {
			maxV = v
		}
	}
	return maxV
}

// NumSegments returns the number of segments covered by this TFR.
// For shard TFRs (§7) this is the shard's segment count; for normal TFRs it
// equals len(snapshot.segment).
func (i *IndexSnapshotTermFieldReader) NumSegments() int {
	return len(i.iterators)
}

// MaxTFNormForSegment returns the max BM25 tf-norm for this term in a specific
// segment. Returns 0 if the term is absent from that segment or avgDocLength≤0.
// Uses the per-TFR cache populated by MaxTFNorm() to avoid redundant invIndexCache
// lookups when initWANDMaxImpacts calls both in sequence.
func (i *IndexSnapshotTermFieldReader) MaxTFNormForSegment(segIdx int, avgDocLength float64) float32 {
	if avgDocLength <= 0 || segIdx >= len(i.dicts) {
		return 0
	}
	if i.segMaxTFNorms != nil && i.segMaxTFNormsAvgDl == avgDocLength && segIdx < len(i.segMaxTFNorms) {
		return i.segMaxTFNorms[segIdx]
	}
	if p, ok := i.dicts[segIdx].(maxTFNormProvider); ok {
		return p.MaxTFNorm(i.term, avgDocLength)
	}
	return 0
}

// SegmentIndexOf returns the shard-relative segment index for the given global
// docID. For normal TFRs this equals the global segment index; for shard TFRs
// (§7) it is global_index − segmentBase.
func (i *IndexSnapshotTermFieldReader) SegmentIndexOf(id index.IndexInternalID) int {
	num, err := id.Value()
	if err != nil {
		return 0
	}
	segIdx, _ := i.snapshot.segmentIndexAndLocalDocNumFromGlobal(num)
	return segIdx - i.segmentBase
}

// FirstDocIDOfSegment returns the first global docID in the shard-relative
// segment segIdx, using buf for the backing storage. Returns nil if segIdx is
// out of range.
func (i *IndexSnapshotTermFieldReader) FirstDocIDOfSegment(segIdx int, buf []byte) index.IndexInternalID {
	globalIdx := i.segmentBase + segIdx
	if globalIdx >= len(i.snapshot.offsets) {
		return nil
	}
	return index.NewIndexInternalID(buf, i.snapshot.offsets[globalIdx])
}

// ShardView creates a lightweight shard TFR covering segments [startSeg, endSeg)
// that borrows dicts and postings (read-only sub-slices) from this TFR and
// allocates only fresh iterators. This avoids the expensive dict/posting setup
// cost of TermFieldReaderForSegmentRange. Used by §7 parallel search via
// TermSearcher.ForSegmentRange. Multiple ShardViews can safely share the same
// TFR's postings concurrently: PostingsList.Iterator() is a read-only operation
// that creates a new independent iterator from the shared mmap'd posting data.
//
// Unadorned TFRs (postings == nil, produced by optimize.go bitmap push-down)
// are handled by copying fresh independent iterators from the parent's
// pre-computed per-segment iterator slice rather than from postings.
func (i *IndexSnapshotTermFieldReader) ShardView(startSeg, endSeg int) (index.TermFieldReader, error) {
	n := endSeg - startSeg
	rv := &IndexSnapshotTermFieldReader{
		term:               i.term,
		field:              i.field,
		snapshot:           i.snapshot,
		segmentBase:        startSeg,
		iterators:          make([]segment.PostingsIterator, n),
		segmentOffset:      0,
		includeFreq:        i.includeFreq,
		includeNorm:        i.includeNorm,
		includeTermVectors: i.includeTermVectors,
		updateBytesRead:    i.updateBytesRead,
		unadorned:          i.unadorned,
		recycle:            false,
		ctx:                i.ctx,
	}
	if len(i.dicts) > 0 {
		rv.dicts = i.dicts[startSeg:endSeg]
	}
	if len(i.postings) > 0 {
		rv.postings = i.postings[startSeg:endSeg]
		for j := 0; j < n; j++ {
			if rv.postings[j] != nil {
				rv.iterators[j] = rv.postings[j].Iterator(i.includeFreq, i.includeNorm, i.includeTermVectors, nil)
			}
		}
	} else {
		// Unadorned path: no postings, but pre-computed bitmap/1-hit iterators
		// per segment set by OptimizeTFRConjunctionUnadorned.Finish (etc.).
		// Each shard goroutine needs its own independent iterator state, so we
		// create a fresh iterator backed by the same read-only bitmap data.
		for j := 0; j < n; j++ {
			if startSeg+j < len(i.iterators) {
				rv.iterators[j] = freshIteratorForShard(i.iterators[startSeg+j])
			}
		}
	}
	atomic.AddUint64(&i.snapshot.parent.stats.TotTermSearchersStarted, uint64(1))
	return rv, nil
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
