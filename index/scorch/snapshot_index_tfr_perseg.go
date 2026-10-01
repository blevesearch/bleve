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

package scorch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// ErrPerSegmentUnsupported is returned by PerSegmentTermFieldReader when at
// least one segment of the snapshot can't hand out block cursors (for example,
// it is of an older segment format). The caller is expected to fall back to
// IndexReader.TermFieldReader.
var ErrPerSegmentUnsupported = errors.New("scorch: segment doesn't support block cursors")

// PerSegmentIndexReader is implemented by index readers that can expose a
// term's postings segment by segment, as plain batches of doc numbers,
// frequencies and norms, instead of the segment-oblivious TermFieldReader.
//
// Doing so lets the caller see (and use) the segment boundaries: it can search
// segments independently, or in parallel, and it works with segment local doc
// numbers.
type PerSegmentIndexReader interface {
	// PerSegmentTermFieldReader returns the readers of a term. The result has
	// one entry per segment of the index, in segment order, and an entry is
	// nil if the term has no postings in that segment. If the term has no
	// postings in any segment, the result is nil.
	//
	// ErrPerSegmentUnsupported is returned if the segments can't be read this
	// way, in which case TermFieldReader should be used instead.
	//
	// withFreqNorms says whether the frequencies and norms of the postings are
	// needed. If they are not (only the matching docs are), they are neither
	// decoded nor handed out: the blocks' Freqs and Norms are unspecified.
	//
	// The readers have to be closed.
	PerSegmentTermFieldReader(ctx context.Context, term []byte, field string,
		withFreqNorms bool) ([]*PerSegmentIndexSnapshotTermFieldReader, error)
}

var _ PerSegmentIndexReader = (*IndexSnapshot)(nil)

// perSegmentGroup is the state shared by all the readers handed out by one
// PerSegmentTermFieldReader call. They account for a single logical term
// reader, just like IndexSnapshotTermFieldReader does.
type perSegmentGroup struct {
	snapshot  *IndexSnapshot
	ctx       context.Context
	open      int32
	bytesRead uint64
}

// PerSegmentIndexSnapshotTermFieldReader reads the postings of one term in one
// segment. It does the job that IndexSnapshotTermFieldReader and the segment's
// PostingsIterator do between them, but is a concrete type that sits directly
// on the segment's block cursor and moves postings one block (of up to
// segment.PostingsBlockLen) at a time. All it hands out are local doc numbers,
// term frequencies and norms.
type PerSegmentIndexSnapshotTermFieldReader struct {
	group *perSegmentGroup

	segmentIndex int
	// offset is added to a local doc number to make it a global one
	offset uint64
	count  uint64
	// segmentDocs is how many docs the segment has, deleted ones included
	segmentDocs uint64

	// pl is the postings list the cursor reads, kept with the cursor as buffers
	// for the next query to reuse (see perSegmentReaderPool)
	pl     segment.PostingsList
	cursor segment.BlockCursor
	// blockMax is the cursor's block-max capability, nil if it hasn't got any
	blockMax segment.BlockMaxCursor

	// blk is the block the cursor last filled; the entries [pos, n) have not
	// been handed out yet.
	blk segment.PostingsBlock
	n   int
	pos int

	// bytes already reported to the group
	reportedBytes uint64
	closed        bool
}

// SegmentIndex is the index of the segment this reader reads.
func (r *PerSegmentIndexSnapshotTermFieldReader) SegmentIndex() int { return r.segmentIndex }

// Offset is what has to be added to a local doc number to get the doc number
// that is unique across the index.
func (r *PerSegmentIndexSnapshotTermFieldReader) Offset() uint64 { return r.offset }

// SegmentDocs is how many docs the segment has, those that were deleted
// included: the doc numbers of the segment are below it.
func (r *PerSegmentIndexSnapshotTermFieldReader) SegmentDocs() uint64 { return r.segmentDocs }

// Count is the number of documents of this segment having the term. Like the
// segment-oblivious reader's count it doesn't discount deletions.
func (r *PerSegmentIndexSnapshotTermFieldReader) Count() uint64 { return r.count }

// LiveCount is the exact number of documents of this segment having the term,
// deletions discounted. It costs next to nothing if the segment has no
// deletions among them, and otherwise it has to walk the doc numbers of the
// postings.
func (r *PerSegmentIndexSnapshotTermFieldReader) LiveCount() (uint64, error) {
	return r.cursor.LiveCount()
}

// HasBlockMax reports whether the reader can bound the scores of its blocks,
// which block-max pruning needs.
func (r *PerSegmentIndexSnapshotTermFieldReader) HasBlockMax() bool { return r.blockMax != nil }

// BoundsAt are the bounds of the first block whose last doc is >= target, and
// whether there is one. It decodes nothing and doesn't move the reader, so it
// can be asked about any target. HasBlockMax has to be true.
func (r *PerSegmentIndexSnapshotTermFieldReader) BoundsAt(target uint32) (segment.BlockBounds, bool) {
	return r.blockMax.BoundsAt(target)
}

// DecodedBounds are the bounds of the block that the last NextBlock or
// SeekBlock decoded.
func (r *PerSegmentIndexSnapshotTermFieldReader) DecodedBounds() segment.BlockBounds {
	return r.blockMax.DecodedBounds()
}

// TermBounds are the bounds of the whole list.
func (r *PerSegmentIndexSnapshotTermFieldReader) TermBounds() segment.BlockBounds {
	return r.blockMax.TermBounds()
}

// SeekBlock decodes the first batch of postings whose local doc number is >=
// target, with the ones below it dropped, as the first n entries of the
// returned block. It never moves backwards. n == 0 means that nothing at or
// after target is left. The block is owned by the reader and is valid until
// the next call on it. Like NextBlock it doesn't mix with Next and Advance.
func (r *PerSegmentIndexSnapshotTermFieldReader) SeekBlock(target uint32) (*segment.PostingsBlock, int, error) {
	n, err := r.cursor.SeekBlock(uint64(target), &r.blk)
	if err != nil {
		return nil, 0, err
	}
	r.pos, r.n = 0, 0
	return &r.blk, n, nil
}

// NextBlock returns the next batch of postings, as the first n entries of the
// returned block. The block is owned by the reader and is valid until the next
// call on it. n == 0 means that the reader is exhausted.
func (r *PerSegmentIndexSnapshotTermFieldReader) NextBlock() (*segment.PostingsBlock, int, error) {
	if r.pos < r.n {
		// a seek or Next left a partial block behind; hand out the rest of it
		if r.pos > 0 {
			rest := r.n - r.pos
			copy(r.blk.Docs[:rest], r.blk.Docs[r.pos:r.n])
			copy(r.blk.Freqs[:rest], r.blk.Freqs[r.pos:r.n])
			copy(r.blk.Norms[:rest], r.blk.Norms[r.pos:r.n])
			r.n = rest
		}
		n := r.n
		r.pos, r.n = 0, 0
		return &r.blk, n, nil
	}
	n, err := r.cursor.NextBlock(&r.blk)
	if err != nil {
		return nil, 0, err
	}
	r.pos, r.n = 0, 0
	return &r.blk, n, nil
}

// Next returns the next posting: its local doc number, term frequency and
// norm. ok is false once the reader is exhausted.
func (r *PerSegmentIndexSnapshotTermFieldReader) Next() (doc, freq uint32,
	norm float32, ok bool, err error) {
	if r.pos >= r.n {
		n, err := r.cursor.NextBlock(&r.blk)
		if err != nil || n == 0 {
			r.pos, r.n = 0, 0
			return 0, 0, 0, false, err
		}
		r.pos, r.n = 0, n
	}
	i := r.pos
	r.pos++
	return r.blk.Docs[i], r.blk.Freqs[i], r.blk.Norms[i], true, nil
}

// Advance returns the first posting whose local doc number is >= target. The
// reader never moves backwards.
func (r *PerSegmentIndexSnapshotTermFieldReader) Advance(target uint32) (doc, freq uint32,
	norm float32, ok bool, err error) {
	// the target may be inside the block that is already buffered
	for r.pos < r.n {
		if r.blk.Docs[r.pos] >= target {
			i := r.pos
			r.pos++
			return r.blk.Docs[i], r.blk.Freqs[i], r.blk.Norms[i], true, nil
		}
		r.pos++
	}
	n, err := r.cursor.SeekBlock(uint64(target), &r.blk)
	if err != nil || n == 0 {
		r.pos, r.n = 0, 0
		return 0, 0, 0, false, err
	}
	r.pos, r.n = 1, n
	return r.blk.Docs[0], r.blk.Freqs[0], r.blk.Norms[0], true, nil
}

// Close releases the reader. The last reader of the group to be closed reports
// the I/O of the whole group, and the group counts as a closed term searcher.
func (r *PerSegmentIndexSnapshotTermFieldReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	g := r.group
	// whatever happens next, this reader is not to be used after Close returns:
	// it goes back to the pool, and so to the next query
	defer r.release()

	if bytesRead := r.cursor.BytesRead(); bytesRead > r.reportedBytes {
		atomic.AddUint64(&g.bytesRead, bytesRead-r.reportedBytes)
		r.reportedBytes = bytesRead
	}

	if atomic.AddInt32(&g.open, -1) == 0 {
		bytesRead := atomic.LoadUint64(&g.bytesRead)
		if g.ctx != nil {
			if statsCallbackFn := g.ctx.Value(search.SearchIOStatsCallbackKey); statsCallbackFn != nil {
				statsCallbackFn.(search.SearchIOStatsCallbackFunc)(bytesRead)
			}
			search.RecordSearchCost(g.ctx, search.AddM, bytesRead)
		}
		atomic.AddUint64(&g.snapshot.parent.stats.TotTermSearchersFinished, uint64(1))
	}
	return nil
}

// perSegmentReaderPool holds readers that have been closed, with the buffers
// they had: the block they decode into, the postings list and the block cursor.
// A reader is a kilobyte and a half and a cursor's decode buffer another one,
// for each segment of each term of each query, and none of it has anything to do
// with the query that it was for.
//
// A reader is put in the pool by Close, and only there; and Close is the last
// thing that is done with it. Whoever calls Close must let go of the reader
// (searchers do), as it may be some other query's the moment it returns. That
// is the whole of the rule that MB-64604 was a breach of, for the classic term
// field readers: one was put back while its holder still had it.
var perSegmentReaderPool = sync.Pool{
	New: func() any { return new(PerSegmentIndexSnapshotTermFieldReader) },
}

// release forgets what a reader was for and puts it in the pool. Its buffers
// stay with it.
func (r *PerSegmentIndexSnapshotTermFieldReader) release() {
	r.group = nil
	r.blockMax = nil
	r.n, r.pos = 0, 0
	r.reportedBytes = 0
	perSegmentReaderPool.Put(r)
}

// PerSegmentDictCacheSize is how many sets of dictionaries (one per segment) a
// snapshot keeps, for each field, for the next queries to use. They are what
// turns the FST of a segment into something a term can be looked up in, and
// building them is most of what the lookup of a term costs in memory. A set is
// used by one query at a time, so as many are made as there are queries at once
// on the field, and as many as this are kept. 0 keeps none.
var PerSegmentDictCacheSize = 8

// perSegmentDictCache is the free sets of dictionaries of a snapshot.
type perSegmentDictCache struct {
	m    sync.Mutex
	free map[string][][]segment.TermDictionary
}

// takePerSegmentDicts hands out a set of dictionaries of the field that nobody
// else has, nil if there is none. Whoever has one has all of it, and puts it
// back when it's done.
func (is *IndexSnapshot) takePerSegmentDicts(field string) []segment.TermDictionary {
	c := &is.perSegmentDicts
	c.m.Lock()
	defer c.m.Unlock()
	sets := c.free[field]
	if len(sets) == 0 {
		return nil
	}
	rv := sets[len(sets)-1]
	sets[len(sets)-1] = nil
	c.free[field] = sets[:len(sets)-1]
	return rv
}

// putPerSegmentDicts takes back a set of dictionaries that is no longer used.
func (is *IndexSnapshot) putPerSegmentDicts(field string, dicts []segment.TermDictionary) {
	if PerSegmentDictCacheSize <= 0 {
		return
	}
	is.parent.rootLock.RLock()
	obsolete := is.parent.root != is
	is.parent.rootLock.RUnlock()
	if obsolete {
		// not the current snapshot any more: nothing will use these for long
		return
	}
	c := &is.perSegmentDicts
	c.m.Lock()
	if c.free == nil {
		c.free = make(map[string][][]segment.TermDictionary)
	}
	if len(c.free[field]) < PerSegmentDictCacheSize {
		c.free[field] = append(c.free[field], dicts)
	}
	c.m.Unlock()
}

// PerSegmentTermFieldReader implements PerSegmentIndexReader.
func (is *IndexSnapshot) PerSegmentTermFieldReader(ctx context.Context, term []byte,
	field string, withFreqNorms bool) ([]*PerSegmentIndexSnapshotTermFieldReader, error) {
	rs := make([]*PerSegmentIndexSnapshotTermFieldReader, len(is.segment))
	// on a way out with an error: the readers go back, unused
	releaseAll := func() {
		for _, r := range rs {
			if r != nil {
				r.release()
			}
		}
	}

	// The dictionaries of the field's segments, from an earlier query if some
	// were kept; they are given back as soon as the postings lists are built,
	// as these don't need them any more.
	dicts := is.takePerSegmentDicts(field)
	fresh := dicts == nil
	if fresh {
		dicts = make([]segment.TermDictionary, len(is.segment))
	}
	dictsComplete := false
	defer func() {
		if dictsComplete {
			is.putPerSegmentDicts(field, dicts)
		}
	}()

	// First the postings lists, to find out if every segment can be read this
	// way before anything is committed to.
	var bytesRead uint64
	nonEmpty := 0
	for i, s := range is.segment {
		// the same accounting TermFieldReader does: metadata of a segment that
		// was mmaped recently is charged once, to the first query on it
		if atomic.CompareAndSwapUint32(&s.mmaped, 1, 0) {
			bytesRead += s.segment.BytesRead()
		}

		if fresh {
			var err error
			// Skip fields that have been completely deleted or had their
			// index data deleted
			if info, ok := is.updatedFields[field]; ok &&
				(info.Index || info.Deleted) {
				dicts[i], err = s.segment.Dictionary("")
			} else {
				dicts[i], err = s.segment.Dictionary(field)
			}
			if err != nil {
				releaseAll()
				return nil, err
			}
			// a dictionary reused has been paid for
			if dictStats, ok := dicts[i].(segment.DiskStatsReporter); ok {
				bytesRead += dictStats.BytesRead()
			}
		}
		dict := dicts[i]

		// the reader has a postings list from its last use, to be filled in anew
		r := perSegmentReaderPool.Get().(*PerSegmentIndexSnapshotTermFieldReader)
		rs[i] = r
		pl, err := dict.PostingsList(term, s.deleted, r.pl)
		if err != nil {
			releaseAll()
			return nil, err
		}
		if _, ok := pl.(segment.BlockCursorProvider); !ok {
			releaseAll()
			return nil, ErrPerSegmentUnsupported
		}
		r.pl = pl
		if plStats, ok := pl.(segment.DiskStatsReporter); ok {
			bytesRead += plStats.BytesRead()
		}
		if pl.Count() == 0 {
			// the term isn't in this segment
			r.release()
			rs[i] = nil
			continue
		}
		nonEmpty++
	}

	dictsComplete = true

	// then the cursors of the segments that have the term
	group := &perSegmentGroup{snapshot: is, ctx: ctx, bytesRead: bytesRead}
	for i, r := range rs {
		if r == nil {
			continue
		}
		cursor, err := r.pl.(segment.BlockCursorProvider).BlockCursor(withFreqNorms, withFreqNorms, r.cursor)
		if err != nil {
			releaseAll()
			return nil, err
		}
		r.cursor = cursor
		r.blockMax, _ = cursor.(segment.BlockMaxCursor)
		r.group = group
		r.segmentIndex = i
		r.offset = is.offsets[i]
		r.count = r.pl.Count()
		r.segmentDocs = is.segment[i].segment.Count()
		r.n, r.pos = 0, 0
		r.reportedBytes = 0
		r.closed = false
		group.open++
	}

	// like TermFieldReader, count every term lookup as a started term searcher.
	// A term that exists nowhere has nothing to close, so count it as finished
	// right away.
	atomic.AddUint64(&is.parent.stats.TotTermSearchersStarted, uint64(1))
	if nonEmpty == 0 {
		atomic.AddUint64(&is.parent.stats.TotTermSearchersFinished, uint64(1))
		if ctx != nil {
			if statsCallbackFn := ctx.Value(search.SearchIOStatsCallbackKey); statsCallbackFn != nil {
				statsCallbackFn.(search.SearchIOStatsCallbackFunc)(bytesRead)
			}
			search.RecordSearchCost(ctx, search.AddM, bytesRead)
		}
		return nil, nil
	}
	return rs, nil
}
