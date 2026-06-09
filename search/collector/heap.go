//  Copyright (c) 2014 Couchbase, Inc.
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

package collector

import (
	"github.com/blevesearch/bleve/v2/search"
)

// collectStoreHeap is a min-heap of DocumentMatches where the root (heap[0])
// is always the *worst* result (lowest score / highest sort key).  Popping the
// root discards the worst element, so AddNotExceedingSize keeps the best k
// documents at all times.
//
// Implemented as a ternary heap (3 children per node) instead of the standard
// library's binary heap.  Height is log₃(n) vs log₂(n), so siftDown is
// shallower and fetches more children per cache line.
type collectStoreHeap struct {
	heap    search.DocumentMatchCollection
	compare collectorCompare
}

func newStoreHeap(capacity int, compare collectorCompare) *collectStoreHeap {
	return &collectStoreHeap{
		heap:    make(search.DocumentMatchCollection, 0, capacity),
		compare: compare,
	}
}

func (c *collectStoreHeap) AddNotExceedingSize(doc *search.DocumentMatch,
	size int) *search.DocumentMatch {
	c.add(doc)
	if len(c.heap) > size {
		return c.removeLast()
	}
	return nil
}

func (c *collectStoreHeap) add(doc *search.DocumentMatch) {
	c.heap = append(c.heap, doc)
	c.siftUp(len(c.heap) - 1)
}

func (c *collectStoreHeap) removeLast() *search.DocumentMatch {
	n := len(c.heap)
	c.heap[0], c.heap[n-1] = c.heap[n-1], c.heap[0]
	result := c.heap[n-1]
	c.heap = c.heap[:n-1]
	if len(c.heap) > 0 {
		c.siftDown(0)
	}
	return result
}

// siftUp restores the heap invariant after appending at index i.
// Moves element at i toward the root while it is less than (worse than) its parent.
func (c *collectStoreHeap) siftUp(i int) {
	h := c.heap
	for i > 0 {
		parent := (i - 1) / 3
		if c.compare(h[i], h[parent]) > 0 { // h[i] is worse → should be closer to root
			h[i], h[parent] = h[parent], h[i]
			i = parent
		} else {
			break
		}
	}
}

// siftDown restores the heap invariant after replacing the root.
// Moves element at i toward the leaves while any child is worse (less) than it.
func (c *collectStoreHeap) siftDown(i int) {
	h := c.heap
	n := len(h)
	for {
		first := 3*i + 1
		if first >= n {
			break
		}
		// Find the worst (least) child among up to three children.
		worst := first
		if s := first + 1; s < n && c.compare(h[s], h[worst]) > 0 {
			worst = s
		}
		if t := first + 2; t < n && c.compare(h[t], h[worst]) > 0 {
			worst = t
		}
		if c.compare(h[worst], h[i]) > 0 { // worst child is worse than current → swap
			h[i], h[worst] = h[worst], h[i]
			i = worst
		} else {
			break
		}
	}
}

func (c *collectStoreHeap) Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error) {
	count := len(c.heap)
	size := count - skip
	if size <= 0 {
		return make(search.DocumentMatchCollection, 0), nil
	}
	rv := make(search.DocumentMatchCollection, size)
	for i := size - 1; i >= 0; i-- {
		doc := c.removeLast()
		rv[i] = doc
		if err := fixup(doc); err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func (c *collectStoreHeap) Len() int {
	return len(c.heap)
}

func (c *collectStoreHeap) Internal() search.DocumentMatchCollection {
	return c.heap
}
