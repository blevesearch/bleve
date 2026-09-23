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

package util

import (
	"math/bits"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// Bitset is a two-level bitmap. The lower level (data) holds one bit per value.
// The upper level (summary) holds one bit per lower-level word, set whenever
// that word is non-empty, so iteration can skip 64 empty words at a time
// instead of loading each one.
//
// That matters because a bitset is sized by the document count of the segment
// it covers, not by the number of bits actually set: without the summary, a
// query matching 0.1% of a five-million-document segment still walks all 78,000
// words to find its 5,000 hits.
//
// The invariant is exact -- a summary bit is set if and only if the
// corresponding data word is non-zero -- so every mutating method below has to
// maintain it. A stale set bit would still be correct, since iteration
// re-checks the word it names and skips it when empty; exactness is what
// preserves the performance the summary exists for.
//
// Maintaining it inside Add costs one extra store per value, against a region
// 64x smaller than the data which stays cache-resident. Measured against the
// same benchmark at four densities on a five-million-document segment, that
// buys 3x at 0.1% and roughly 1.1x at 1%, breaks even at 10%, and costs about
// 9% at 50% where nearly every word is occupied and there is nothing to skip.
// Two alternatives were tried and rejected: making the store conditional on the
// word having been empty regressed 10% density by 19% on an unpredictable
// branch, and rebuilding the summary in one pass per iteration regressed 1%
// density by 27% by paying an O(words) pass on top of the walk.
type Bitset struct {
	// backing owns the memory; data and summary are views over it, so that a
	// pooled bitset is a single allocation.
	backing []uint64
	data    []uint64
	summary []uint64
	numBits int
	exclude *roaring.Bitmap
}

// bitsetSizes returns the number of lower-level and upper-level words needed to
// hold values up to maxVal.
func bitsetSizes(maxVal int) (words, summaryWords int) {
	// We need (maxVal / 64) + 1 buckets to hold up to maxVal
	words = (maxVal / 64) + 1
	// and one summary bit per bucket, 64 buckets to a summary word
	summaryWords = ((words - 1) / 64) + 1
	return words, summaryWords
}

// split carves the two levels out of one backing slice. data is capped at its
// own length so that an accidental append cannot scribble over the summary.
func (b *Bitset) split(backing []uint64, words int) {
	b.backing = backing
	b.data = backing[:words:words]
	b.summary = backing[words:]
}

// NewBitset initializes a bitset capable of holding numbers up to maxVal
func NewBitset(maxVal int, exclude *roaring.Bitmap) *Bitset {
	words, summaryWords := bitsetSizes(maxVal)
	rv := &Bitset{
		numBits: maxVal,
		exclude: exclude,
	}
	rv.split(make([]uint64, words+summaryWords), words)
	return rv
}

// Add inserts a value into the bitset (safely handles duplicates)
func (b *Bitset) Add(val int) {
	if b.exclude != nil && b.exclude.Contains(uint32(val)) {
		return
	}
	bucket := val >> 6    // Equivalent to val / 64
	bit := uint(val & 63) // Equivalent to val % 64

	// Set the bit to 1 using bitwise OR
	b.data[bucket] |= (1 << bit)

	// and mark the bucket as occupied, so iteration can skip 64 empty buckets
	// at a time instead of loading each one
	b.summary[bucket>>6] |= 1 << uint(bucket&63)
}

// Remove deletes a value from the bitset
func (b *Bitset) Remove(val int) {
	bucket := val >> 6
	bit := uint(val & 63)

	// Set the bit to 0 using bitwise AND with the complement
	b.data[bucket] &^= (1 << bit)

	// keep the summary exact: clear its bit once the bucket empties
	if b.data[bucket] == 0 {
		b.summary[bucket>>6] &^= 1 << uint(bucket&63)
	}
}

// Contains checks if a value exists in the bitset
func (b *Bitset) Contains(val int) bool {
	bucket := val >> 6
	bit := uint(val & 63)

	return (b.data[bucket] & (1 << bit)) != 0
}

// Invert flips all bits in the bitset,
// effectively turning all 1s to 0s and vice versa
func (b *Bitset) Invert() {
	for i := range b.data {
		b.data[i] = ^b.data[i]
	}
	// the flip above sets the trailing bits beyond numBits in the last
	// bucket(s), which do not correspond to valid values - clear them so
	// that Iterate and Count never see them
	lastBucket := b.numBits >> 6
	if lastBucket < len(b.data) {
		b.data[lastBucket] &= (1 << uint(b.numBits&63)) - 1
		for i := lastBucket + 1; i < len(b.data); i++ {
			b.data[i] = 0
		}
	}
	if b.exclude != nil {
		it := b.exclude.Iterator()
		for it.HasNext() {
			bit := uint64(it.Next())
			word := bit / 64
			if word < uint64(len(b.data)) {
				b.data[word] &^= uint64(1) << (bit % 64)
			}
		}
	}

	// every word just changed; Invert is already O(words) so rebuilding here
	// rather than deferring costs nothing extra
	b.rebuildSummary()
}

// rebuildSummary recomputes the upper level from the lower one. Only needed
// after a bulk rewrite of data; Add and Remove maintain it incrementally.
func (b *Bitset) rebuildSummary() {
	clear(b.summary)
	for i, word := range b.data {
		if word != 0 {
			b.summary[i>>6] |= 1 << uint(i&63)
		}
	}
}

// Iterate calls the provided function for every integer recorded in the bitset, in ascending order
func (b *Bitset) Iterate(f func(int)) {
	for summaryIdx, summaryWord := range b.summary {
		// each set summary bit names a non-empty data word; empty words are
		// skipped 64 at a time
		for summaryWord != 0 {
			bucketIdx := summaryIdx<<6 + bits.TrailingZeros64(summaryWord)
			summaryWord &= summaryWord - 1

			// walk only the set bits of the bucket, lowest first
			for bucket := b.data[bucketIdx]; bucket != 0; bucket &= bucket - 1 {
				f(bucketIdx<<6 + bits.TrailingZeros64(bucket))
			}
		}
	}
}

func (b *Bitset) Count() int {
	count := 0

	for _, word := range b.data {
		count += bits.OnesCount64(word)
	}

	return count
}

func (b *Bitset) Clear() {
	clear(b.data)
	clear(b.summary)
}

// SizeInBytes returns the memory footprint of the bitset's backing words.
func (b *Bitset) SizeInBytes() int {
	if b == nil {
		return 0
	}
	return len(b.backing) * 8
}

// -----------------------------------------------------------------------------
// iteration

// BitsetIterator walks the set bits of a Bitset in ascending order, using the
// summary level to skip runs of empty words.
//
// It is a value type on purpose: the caller keeps it in a slice and calls
// through it once per hit, so an interface or a pointer chase per call would
// cost more than the bit extraction itself. The zero value is a valid, empty
// iterator, which lets callers leave a slot unset rather than nil-checking on
// the hot path.
type BitsetIterator struct {
	words   []uint64
	summary []uint64

	// summaryIdx is the summary word being consumed, and summaryWord its bits
	// that have not been visited yet; each names a non-empty data word.
	summaryIdx  int
	summaryWord uint64

	// wordIdx is the data word being consumed, and word its bits that have not
	// been returned yet, so the lowest set bit is always the next value.
	wordIdx int
	word    uint64
}

// Iterator returns an iterator over the bitset's set bits. It aliases the
// bitset's memory, so it must not be used after the bitset is released.
func (b *Bitset) Iterator() BitsetIterator {
	rv := BitsetIterator{words: b.data, summary: b.summary}
	if len(b.summary) > 0 {
		rv.summaryWord = b.summary[0]
	}
	return rv
}

// Next returns the next set bit in ascending order, or false once exhausted.
func (it *BitsetIterator) Next() (int, bool) {
	for it.word == 0 {
		if !it.nextWord() {
			return 0, false
		}
	}

	bit := bits.TrailingZeros64(it.word)
	// clear the lowest set bit
	it.word &= it.word - 1

	return it.wordIdx<<6 + bit, true
}

// nextWord loads the next non-empty data word, consuming one summary bit and
// advancing through summary words as needed. Returns false once exhausted.
func (it *BitsetIterator) nextWord() bool {
	for it.summaryWord == 0 {
		it.summaryIdx++
		if it.summaryIdx >= len(it.summary) {
			// clamp, so repeated calls past the end stay put
			it.summaryIdx = len(it.summary)
			return false
		}
		it.summaryWord = it.summary[it.summaryIdx]
	}

	it.wordIdx = it.summaryIdx<<6 + bits.TrailingZeros64(it.summaryWord)
	it.summaryWord &= it.summaryWord - 1
	it.word = it.words[it.wordIdx]
	return true
}

// AdvanceTo positions the iterator so that the next call to Next returns the
// first set bit greater than or equal to val. It only ever moves forward: a val
// at or behind the current position leaves the iterator untouched.
func (it *BitsetIterator) AdvanceTo(val int) {
	if val < 0 {
		return
	}

	targetWord := val >> 6
	if targetWord < it.wordIdx {
		// already past it
		return
	}

	if targetWord == it.wordIdx && it.word != 0 {
		// still inside the word we are partway through: dropping the bits
		// below val is enough, unless that empties it
		it.word &= ^uint64(0) << uint(val&63)
		if it.word != 0 {
			return
		}
	}

	// the current word cannot serve val, so move the summary to the word
	// containing val and let Next pick up from there
	it.word = 0

	targetSummary := targetWord >> 6
	if targetSummary >= len(it.summary) {
		it.summaryIdx = len(it.summary)
		it.summaryWord = 0
		return
	}
	if targetSummary > it.summaryIdx {
		it.summaryIdx = targetSummary
		it.summaryWord = it.summary[targetSummary]
	}
	if it.summaryIdx == targetSummary {
		// drop the summary bits for words below the target
		it.summaryWord &= ^uint64(0) << uint(targetWord&63)
	}

	if !it.nextWord() {
		return
	}
	if it.wordIdx == targetWord {
		it.word &= ^uint64(0) << uint(val&63)
	}
}

// -----------------------------------------------------------------------------
// pooling

// bitsetPool recycles the backing word slices. A bitset is sized by the
// document count of the segment it covers, not by the number of bits actually
// set, so a selective query allocates just as much as a broad one. Reusing the
// words keeps that cost off the allocator entirely.
//
// Pointers to slices are pooled rather than slices themselves, so that handing
// one to sync.Pool does not itself allocate an interface box.
var bitsetPool sync.Pool

// AcquireBitset returns a zeroed bitset capable of holding values up to maxVal,
// reusing pooled memory when a large enough slice is available.
//
// The caller owns the bitset and should Release it once done. Anything derived
// from it -- in particular an Iterator, which aliases the same words -- must
// not be used after Release.
func AcquireBitset(maxVal int, exclude *roaring.Bitmap) *Bitset {
	// both levels come out of one allocation, so the pooled slice has to be
	// large enough for the summary as well as the data
	words, summaryWords := bitsetSizes(maxVal)
	total := words + summaryWords

	var backing []uint64
	if v := bitsetPool.Get(); v != nil {
		pooled := v.(*[]uint64)
		if cap(*pooled) >= total {
			backing = (*pooled)[:total]
			clear(backing)
		} else {
			// too small to serve this request, but still worth keeping
			bitsetPool.Put(pooled)
		}
	}
	if backing == nil {
		backing = make([]uint64, total)
	}

	rv := &Bitset{
		numBits: maxVal,
		exclude: exclude,
	}
	rv.split(backing, words)
	return rv
}

// Release returns the bitset's memory to the pool. The bitset is left empty, so
// a later Add or Contains will panic rather than corrupt recycled memory, and a
// second Release is a no-op.
func (b *Bitset) Release() {
	if b == nil || b.backing == nil {
		return
	}
	// return the whole backing allocation, not b.data: that view is cap-limited
	// to the data level and excludes the summary, so pooling it would make every
	// later acquire fail the capacity check and allocate instead
	backing := b.backing
	b.backing = nil
	b.data = nil
	b.summary = nil
	bitsetPool.Put(&backing)
}
