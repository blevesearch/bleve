//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package scorch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bits-and-blooms/bitset"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

func (is *IndexSnapshot) VectorReader(ctx context.Context, vector []float32,
	field string, k int64, searchParams json.RawMessage,
	eligibleSelector index.EligibleDocumentSelector) (
	index.VectorReader, error) {
	rv := &IndexSnapshotVectorReader{
		vector:           vector,
		field:            field,
		k:                k,
		snapshot:         is,
		searchParams:     searchParams,
		eligibleSelector: eligibleSelector,
		postings:         make([]segment_api.VecPostingsList, len(is.segment)),
		iterators:        make([]segment_api.VecPostingsIterator, len(is.segment)),
	}

	// initialize postings and iterators within the OptimizeVR's Finish()
	return rv, nil
}

// eligibleDocumentList represents the list of eligible documents within a segment.
type eligibleDocumentList struct {
	bs *bitset.BitSet
}

// Iterator returns an iterator for the eligible document IDs.
func (edl *eligibleDocumentList) Iterator() index.EligibleDocumentIterator {
	if edl.bs == nil {
		// no eligible documents
		return emptyEligibleIterator
	}
	// return the iterator
	return &eligibleDocumentIterator{
		bs: edl.bs,
	}
}

// Count returns the number of eligible document IDs.
func (edl *eligibleDocumentList) Count() uint64 {
	if edl.bs == nil {
		return 0
	}
	return uint64(edl.bs.Count())
}

// emptyEligibleDocumentList is a reusable empty eligible document list.
var emptyEligibleDocumentList = &eligibleDocumentList{}

// eligibleDocumentIterator iterates over eligible document IDs within a segment.
type eligibleDocumentIterator struct {
	bs      *bitset.BitSet
	current uint
}

// Next returns the next eligible document ID and whether it exists.
func (it *eligibleDocumentIterator) Next() (id uint64, ok bool) {
	next, found := it.bs.NextSet(it.current)
	if !found {
		return 0, false
	}
	it.current = next + 1
	return uint64(next), true
}

// emptyEligibleIterator is a reusable empty eligible document iterator.
var emptyEligibleIterator = &emptyEligibleDocumentIterator{}

// emptyEligibleDocumentIterator is an iterator that always returns no documents.
type emptyEligibleDocumentIterator struct{}

// Next always returns false for empty iterator.
func (it *emptyEligibleDocumentIterator) Next() (id uint64, ok bool) {
	return 0, false
}

// eligibleDocumentSelector is used to filter out documents that are eligible for
// the KNN search from a pre-filter query.
type eligibleDocumentSelector struct {
	// segment ID -> segment local doc nums in a bitset
	eligibleDocNums []*bitset.BitSet
	is              *IndexSnapshot
}

// SegmentEligibleDocuments returns an EligibleDocumentList for the specified segment ID.
func (eds *eligibleDocumentSelector) SegmentEligibleDocuments(segmentID int) index.EligibleDocumentList {
	if eds.eligibleDocNums == nil || segmentID < 0 || segmentID >= len(eds.eligibleDocNums) {
		return emptyEligibleDocumentList
	}
	bs := eds.eligibleDocNums[segmentID]
	if bs == nil {
		// no eligible documents for this segment
		return emptyEligibleDocumentList
	}
	return &eligibleDocumentList{
		bs: bs,
	}
}

// AddEligibleDocumentMatch adds a document match to the list of eligible documents.
func (eds *eligibleDocumentSelector) AddEligibleDocumentMatch(id index.IndexInternalID) error {
	if eds.is == nil {
		return fmt.Errorf("eligibleDocumentSelector is not initialized with IndexSnapshot")
	}
	// Get the segment number and the local doc number for this document.
	segIdx, docNum, err := eds.is.segmentIndexAndLocalDocNum(id)
	if err != nil {
		return err
	}
	// allocate a bitset for this segment if needed
	if eds.eligibleDocNums[segIdx] == nil {
		// the size of the bitset is the full size of the segment (which is the max local doc num + 1)
		eds.eligibleDocNums[segIdx] = bitset.New(uint(eds.is.segment[segIdx].FullSize()))
	}
	// Add the local doc number to the list of eligible doc numbers for this segment.
	eds.eligibleDocNums[segIdx].Set(uint(docNum))
	return nil
}

func (is *IndexSnapshot) NewEligibleDocumentSelector() index.EligibleDocumentSelector {
	return &eligibleDocumentSelector{
		eligibleDocNums: make([]*bitset.BitSet, len(is.segment)),
		is:              is,
	}
}
