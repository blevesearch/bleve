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

package util

import (
	"math/bits"

	"github.com/RoaringBitmap/roaring/v2"
)

type Bitset struct {
	data    []uint64
	exclude *roaring.Bitmap
}

// NewBitset initializes a bitset capable of holding numbers up to maxVal
func NewBitset(maxVal int, exclude *roaring.Bitmap) *Bitset {
	// We need (maxVal / 64) + 1 buckets to hold up to maxVal
	size := (maxVal / 64) + 1
	return &Bitset{
		data:    make([]uint64, size),
		exclude: exclude,
	}
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
}

// Remove deletes a value from the bitset
func (b *Bitset) Remove(val int) {
	bucket := val >> 6
	bit := uint(val & 63)

	// Set the bit to 0 using bitwise AND with the complement
	b.data[bucket] &^= (1 << bit)
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
}

// Iterate calls the provided function for every integer recorded in the bitset, in ascending order
func (b *Bitset) Iterate(f func(int)) {
	for bucketIdx, bucket := range b.data {
		// If the entire 64-bit block is 0, skip it entirely for speed
		if bucket == 0 {
			continue
		}

		// Check all 64 bits in this bucket
		for bitIdx := 0; bitIdx < 64; bitIdx++ {
			if (bucket & (1 << uint(bitIdx))) != 0 {
				// Reconstruct the original integer
				originalVal := (bucketIdx << 6) + bitIdx
				f(originalVal)
			}
		}
	}
}

func (b *Bitset) Count() int {
	count := 0

	// Explicitly range over the slice. The Go compiler optimizes this loop,
	// often utilizing SIMD or unrolling where appropriate.
	for _, word := range b.data {
		count += bits.OnesCount64(word)
	}

	return count
}

func (b *Bitset) Clear() {
	for i := range b.data {
		b.data[i] = 0
	}
}
