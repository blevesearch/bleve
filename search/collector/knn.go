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

type collectStoreKNN struct {
	internalHeaps  []collectorStore
	sizes          []int64
	totalScoreHeap collectorStore
	ejectedDocs    []*search.DocumentMatch
	scoreCorrector func(docMatch *search.DocumentMatch)
}

// size is the from+size used for the tf-idf and total score heaps.
func newStoreKNN(totalScoreHeap collectorStore, sizes []int64, internalHeaps []collectorStore,
	scoreCorrector func(docMatch *search.DocumentMatch)) *collectStoreKNN {
	return &collectStoreKNN{
		internalHeaps:  internalHeaps,
		totalScoreHeap: totalScoreHeap,
		sizes:          sizes,
		ejectedDocs:    make([]*search.DocumentMatch, len(internalHeaps)),
		scoreCorrector: scoreCorrector,
	}
}

func (c *collectStoreKNN) AddNotExceedingSize(doc *search.DocumentMatch, size int) []*search.DocumentMatch {
	numDocsEjected := 0

	for heapIdx := 1; heapIdx < len(c.internalHeaps); heapIdx++ {
		if doc.ScoreBreakdown[heapIdx] == 0 {
			continue
		}
		ejectedDoc := c.internalHeaps[heapIdx].AddNotExceedingSize(doc, int(c.sizes[heapIdx]))
		if ejectedDoc != nil {
			delete(ejectedDoc[0].ScoreBreakdown, heapIdx)
			c.ejectedDocs[numDocsEjected] = ejectedDoc[0]
			numDocsEjected++
		}
	}
	if doc.ScoreBreakdown[0] != 0 {
		tfIdfEject := c.internalHeaps[0].AddNotExceedingSize(doc, int(c.sizes[0]))
		if tfIdfEject != nil && len(tfIdfEject[0].ScoreBreakdown) == 1 {
			delete(tfIdfEject[0].ScoreBreakdown, 0)
			c.ejectedDocs[numDocsEjected] = tfIdfEject[0]
			numDocsEjected++
		}
	}

	for idx := 0; idx < numDocsEjected; idx++ {
		c.scoreCorrector(c.ejectedDocs[idx])
	}

	c.totalScoreHeap.Add(doc)
	rv := c.totalScoreHeap.PopWhile(func(doc *search.DocumentMatch) bool {
		return doc.Score == 0
	})
	return rv
}

func (c *collectStoreKNN) Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error) {
	if c.totalScoreHeap.Len() <= skip {
		// heap is smaller than skip, return empty
		return make(search.DocumentMatchCollection, 0), nil
	}
	for i := 0; i < skip; i++ {
		// remove lowest skip elements of the total score heap - min heap
		c.totalScoreHeap.Remove()
	}
	// need to return min(size, len(c.pq)) elements now
	size := int(c.sizes[0]) - skip
	if size > c.totalScoreHeap.Len() {
		size = c.totalScoreHeap.Len()
	}
	rv := make(search.DocumentMatchCollection, size)
	for i := 0; i < size; i++ {
		doc := c.totalScoreHeap.Remove()
		err := fixup(doc)
		if err != nil {
			return nil, err
		}
		rv[i] = doc
	}
	return rv, nil
}

func (c *collectStoreKNN) PopWhile(popCondition func(doc *search.DocumentMatch) bool) []*search.DocumentMatch {
	// not impl here
	return nil
}
func (c *collectStoreKNN) Add(doc *search.DocumentMatch) {
	// not impl here
}
func (c *collectStoreKNN) Remove() *search.DocumentMatch {
	// not impl here
	return nil
}

func (c *collectStoreKNN) Len() int {
	// not impl here
	return 0
}
