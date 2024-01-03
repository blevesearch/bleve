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
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

const VectorSearchSupportedSegmentVersion = 16

var reflectStaticSizeIndexSnapshotVectorReader int

func init() {
	var istfr IndexSnapshotVectorReader
	reflectStaticSizeIndexSnapshotVectorReader = int(reflect.TypeOf(istfr).Size())
}

type IndexSnapshotVectorReader struct {
	vector        []float32
	field         string
	k             int64
	snapshot      *IndexSnapshot
	postings      []segment_api.VecPostingsList
	iterators     []segment_api.VecPostingsIterator
	segmentOffset int
	currPosting   segment_api.VecPosting
	currID        index.IndexInternalID
	ctx           context.Context
}

func (i *IndexSnapshotVectorReader) Size() int {
	sizeInBytes := reflectStaticSizeIndexSnapshotVectorReader + size.SizeOfPtr +
		len(i.vector) + len(i.field) + len(i.currID)

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

func (i *IndexSnapshotVectorReader) Next(preAlloced *index.VectorDoc) (
	*index.VectorDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.VectorDoc{}
	}

	for i.segmentOffset < len(i.iterators) {
		next, err := i.iterators[i.segmentOffset].Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			// make segment number into global number by adding offset
			globalOffset := i.snapshot.offsets[i.segmentOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)
			rv.Score = float64(next.Score())

			i.currID = rv.ID
			i.currPosting = next

			return rv, nil
		}
		i.segmentOffset++
	}

	return nil, nil
}

func (i *IndexSnapshotVectorReader) Advance(ID index.IndexInternalID,
	preAlloced *index.VectorDoc) (*index.VectorDoc, error) {

	if i.currPosting != nil && bytes.Compare(i.currID, ID) >= 0 {
		i2, err := i.snapshot.VectorReader(i.ctx, i.vector, i.field, i.k)
		if err != nil {
			return nil, err
		}
		// close the current term field reader before replacing it with a new one
		_ = i.Close()
		*i = *(i2.(*IndexSnapshotVectorReader))
	}

	num, err := docInternalToNumber(ID)
	if err != nil {
		return nil, fmt.Errorf("error converting to doc number % x - %v", ID, err)
	}
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
		preAlloced = &index.VectorDoc{}
	}
	preAlloced.ID = docNumberToBytes(preAlloced.ID, next.Number()+
		i.snapshot.offsets[segIndex])
	i.currID = preAlloced.ID
	i.currPosting = next
	return preAlloced, nil
}

func (i *IndexSnapshotVectorReader) Count() uint64 {
	var rv uint64
	for _, posting := range i.postings {
		rv += posting.Count()
	}
	return rv
}

func (i *IndexSnapshotVectorReader) Close() error {
	// TODO Consider if any scope of recycling here.
	return nil
}
