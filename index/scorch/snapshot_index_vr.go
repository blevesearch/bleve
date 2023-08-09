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

//go:build densevector
// +build densevector

package scorch

import (
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

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
			// TODO Invoke next.Score() and make it part of vector doc.
			rv.ID = docNumberToBytes(rv.ID, nnum+globalOffset)

			i.currID = rv.ID
			i.currPosting = next

			return rv, nil
		}
		i.segmentOffset++
	}

	return rv, nil
}

func (i *IndexSnapshotVectorReader) Advance(ID index.IndexInternalID,
	preAlloced *index.VectorDoc) (*index.VectorDoc, error) {

	return &index.VectorDoc{}, nil
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
