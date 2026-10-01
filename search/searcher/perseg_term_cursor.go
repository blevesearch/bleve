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
	"math"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search/scorer"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// noMoreDocs is the doc of a cursor that has run out of postings.
const noMoreDocs = uint32(math.MaxUint32)

// termCursor walks the postings of one term in one segment a document at a
// time, over the blocks the segment hands out. It is what the composite per
// segment searchers (conjunction, disjunction) are made of.
//
// On top of Doc, Advance, Seek and Score it has the block-max side that
// pruning needs: ShallowSeek says what block a doc would be in, and how much
// that block can score at most, without decoding it and without moving the
// cursor. So whatever is learnt about faraway blocks, the cursor keeps
// reading where it is.
//
// Scores are float32, those of ScoreBlock to the bit.
type termCursor struct {
	// idx is the place of the term among the terms of the query: scores are
	// summed in that order, whatever order the cursors are visited in, so that
	// every algorithm gives a doc the very same score.
	idx    int
	offset uint64

	reader *scorch.PerSegmentIndexSnapshotTermFieldReader
	scorer *scorer.PerSegmentTermScorer
	// scored is whether the block has frequencies and norms
	scored bool

	blk    *segment.PostingsBlock
	n, pos int
	doc    uint32
	// cur are the bounds of the decoded block, if the reader can tell
	cur segment.BlockBounds

	// The block ShallowSeek last looked at, and the most it can score. It is the
	// block of every doc from shallowTarget to its last doc, so as long as
	// the targets keep going up and stay in it, there's nothing to look up.
	shallow       segment.BlockBounds
	shallowMax    float32
	shallowTarget uint32
	shallowValid  bool

	maxScore float32
	cost     uint64
	err      error

	// the scores of the whole decoded block, once someone has asked for them
	scores    *[scorer.PerSegmentBlockLen]float32 // allocated when first asked for
	scoresFor uint64                              // the block generation that scores is of; 0 for none
	blockGen  uint64                              // changes with every block decoded
}

// newTermCursor positions a cursor on the first posting of the reader.
func newTermCursor(idx int, r *scorch.PerSegmentIndexSnapshotTermFieldReader,
	sc *scorer.PerSegmentTermScorer, scored bool) *termCursor {
	t := &termCursor{
		idx:    idx,
		offset: r.Offset(),
		reader: r,
		scorer: sc,
		scored: scored,
		cost:   r.Count(),
	}
	if scored && r.HasBlockMax() {
		tb := r.TermBounds()
		t.maxScore = sc.UpperBound(tb.MaxFreq, tb.MaxNorm, tb.FreqBounded)
	}
	t.seekBlock(0)
	return t
}

// Offset is what has to be added to a doc number to make it unique across the
// index.
func (t *termCursor) Offset() uint64 { return t.offset }

// blockScores are the scores of the decoded block's postings, all of them
// at once, by the block kernel. Entry i is that of blk.Docs[i]. They are
// worked out once per block.
func (t *termCursor) blockScores() *[scorer.PerSegmentBlockLen]float32 {
	if t.scores == nil {
		t.scores = new([scorer.PerSegmentBlockLen]float32)
	}
	if t.scoresFor != t.blockGen {
		t.scorer.ScoreBlock(&t.blk.Freqs, &t.blk.Norms, t.n, t.scores)
		t.scoresFor = t.blockGen
	}
	return t.scores
}

// Doc is the doc number the cursor is on, noMoreDocs if it's done.
func (t *termCursor) Doc() uint32 { return t.doc }

// Cost is how many postings the term has in the segment: how expensive it
// is to walk.
func (t *termCursor) Cost() uint64 { return t.cost }

// MaxScore is the most a posting of the term can score.
func (t *termCursor) MaxScore() float32 { return t.maxScore }

// Err is the error that made the cursor give up, if it did.
func (t *termCursor) Err() error { return t.err }

// Score is the score of the posting the cursor is on.
func (t *termCursor) Score() float32 {
	return t.scorer.ScoreOne(t.blk.Freqs[t.pos], t.blk.Norms[t.pos])
}

// Advance moves to the next posting.
func (t *termCursor) Advance() uint32 {
	if t.doc == noMoreDocs {
		return noMoreDocs
	}
	if t.pos+1 < t.n {
		t.pos++
		t.doc = t.blk.Docs[t.pos]
		return t.doc
	}
	if t.doc == noMoreDocs-1 {
		return t.finish()
	}
	t.seekBlock(t.doc + 1)
	return t.doc
}

// Seek moves to the first posting >= target. The cursor never moves backwards:
// a target at or below the current doc leaves it where it is.
func (t *termCursor) Seek(target uint32) uint32 {
	if t.doc == noMoreDocs || target <= t.doc {
		return t.doc
	}
	docs := t.blk.Docs[:t.n]
	// in the decoded block?
	if target > docs[len(docs)-1] {
		t.seekBlock(target)
		return t.doc
	}

	// Seeks come in ascending order and usually a short way apart: the very
	// next posting is the answer more often than not, and is worth a look
	// before a search.
	pos := t.pos + 1 // in range: target is above the current doc and not above the last
	if docs[pos] >= target {
		t.pos = pos
		t.doc = docs[pos]
		return t.doc
	}

	// The first posting >= target is in (pos, len(docs)), and the last one is
	// one. The search has no branch on the comparison -- the compiler turns the
	// conditional step into a select -- as a branch here is a coin toss for the
	// predictor, and costs more than the whole search.
	base := pos + 1
	n := len(docs) - base
	for n > 1 {
		half := n >> 1
		if docs[base+half-1] < target {
			base += half
		}
		n -= half
	}
	t.pos = base
	t.doc = docs[base]
	return t.doc
}

func (t *termCursor) finish() uint32 {
	t.doc = noMoreDocs
	t.n, t.pos = 0, 0
	return noMoreDocs
}

// seekBlock decodes the first postings that are >= target, and goes to the
// first of them.
func (t *termCursor) seekBlock(target uint32) {
	blk, n, err := t.reader.SeekBlock(target)
	if err != nil {
		t.err = err
		t.finish()
		return
	}
	if n == 0 {
		t.finish()
		return
	}
	t.blk, t.n, t.pos = blk, n, 0
	t.blockGen++
	t.doc = blk.Docs[0]
	if t.reader.HasBlockMax() {
		t.cur = t.reader.DecodedBounds()
	}
}

// ShallowSeek looks at the block that holds the first posting >= target, and
// remembers its bounds for LastDocInBlock and BlockMaxScore. The cursor itself
// doesn't move and nothing is decoded. Needs the reader to have block-max
// data.
func (t *termCursor) ShallowSeek(target uint32) {
	if t.shallowValid && target >= t.shallowTarget && target <= t.shallow.LastDoc {
		return // the same block
	}
	if t.doc != noMoreDocs && target <= t.cur.LastDoc {
		// the block the cursor is in already
		t.shallow = t.cur
	} else if bd, ok := t.reader.BoundsAt(target); ok {
		t.shallow = bd
	} else {
		t.shallow = segment.BlockBounds{LastDoc: noMoreDocs, FreqBounded: true}
	}
	t.shallowTarget, t.shallowValid = target, true
	t.shallowMax = t.scorer.UpperBound(t.shallow.MaxFreq, t.shallow.MaxNorm, t.shallow.FreqBounded)
}

// LastDocInBlock is the last doc of the block ShallowSeek looked at
// (noMoreDocs if there is none).
func (t *termCursor) LastDocInBlock() uint32 { return t.shallow.LastDoc }

// BlockMaxScore is the most a posting of the block ShallowSeek looked at can
// score; 0 if there is no such block.
func (t *termCursor) BlockMaxScore() float32 {
	if t.shallow.LastDoc == noMoreDocs {
		return 0
	}
	return t.shallowMax
}

// unionWindowDocs is how many docs a window of a union covers, the bits of
// unionWindowWords words.
const (
	unionWindowWords = 64
	unionWindowDocs  = unionWindowWords * 64
)

// fillBits sets, in words, the bit of every doc of the cursor that is in
// [start, end), the bit of doc d being d-start, and moves the cursor on to its
// first doc >= end. The cursor has to be on a doc >= start, and words has to
// have a bit for each doc of the window.
func (t *termCursor) fillBits(start, end uint32, words []uint64) {
	for t.doc != noMoreDocs && t.doc < end {
		i := t.pos
		for ; i < t.n; i++ {
			d := t.blk.Docs[i]
			if d >= end {
				break
			}
			rel := d - start
			words[rel>>6] |= 1 << (rel & 63)
		}
		if i < t.n {
			t.pos = i
			t.doc = t.blk.Docs[i]
			return
		}
		// the block is used up
		last := t.blk.Docs[t.n-1]
		if last == noMoreDocs-1 {
			t.finish()
			return
		}
		t.seekBlock(last + 1)
	}
}
