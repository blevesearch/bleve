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

package zap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/size"
	"github.com/golang/snappy"
)

var reflectStaticSizedocValueReader int

func init() {
	var dvi docValueReader
	reflectStaticSizedocValueReader = int(reflect.TypeOf(dvi).Size())
}

type docValueReader struct {
	field          string
	curChunkNum    uint64
	numChunks      uint64
	chunkOffsets   []uint64
	dvDataLoc      uint64
	curChunkHeader []MetaData
	curChunkData   []byte // compressed data cache
	uncompressed   []byte // temp buf for snappy decompression
}

func (di *docValueReader) size() int {
	return reflectStaticSizedocValueReader + size.SizeOfPtr +
		len(di.field) +
		len(di.chunkOffsets)*size.SizeOfUint64 +
		len(di.curChunkHeader)*reflectStaticSizeMetaData +
		len(di.curChunkData)
}

func (di *docValueReader) fieldName() string {
	return di.field
}

func (di *docValueReader) curChunkNumber() uint64 {
	return di.curChunkNum
}

func (s *SegmentBase) loadFieldDocValueReader(field string,
	fieldDvLoc uint64) (*docValueReader, error) {
	// get the docValue offset for the given fields
	if fieldDvLoc == fieldNotUninverted {
		return nil, fmt.Errorf("loadFieldDocValueReader: "+
			"no docValues found for field: %s", field)
	}

	// read the number of chunks, chunk lengths
	var offset, loc uint64
	numChunks, read := binary.Uvarint(s.mem[fieldDvLoc : fieldDvLoc+binary.MaxVarintLen64])
	if read <= 0 {
		return nil, fmt.Errorf("failed to read the field "+
			"doc values for field %s", field)
	}
	offset += uint64(read)

	fdvIter := &docValueReader{
		curChunkNum:  math.MaxUint64,
		field:        field,
		chunkOffsets: make([]uint64, int(numChunks)),
	}
	for i := 0; i < int(numChunks); i++ {
		loc, read = binary.Uvarint(s.mem[fieldDvLoc+offset : fieldDvLoc+offset+binary.MaxVarintLen64])
		if read <= 0 {
			return nil, fmt.Errorf("corrupted chunk offset during segment load")
		}
		fdvIter.chunkOffsets[i] = loc
		offset += uint64(read)
	}

	fdvIter.dvDataLoc = fieldDvLoc + offset
	return fdvIter, nil
}

func (di *docValueReader) loadDvChunk(chunkNumber,
	localDocNum uint64, s *SegmentBase) error {
	// advance to the chunk where the docValues
	// reside for the given docNum
	destChunkDataLoc, curChunkEnd := di.dvDataLoc, di.dvDataLoc
	start, end := readChunkBoundary(int(chunkNumber), di.chunkOffsets)
	destChunkDataLoc += start
	curChunkEnd += end

	// read the number of docs reside in the chunk
	numDocs, read := binary.Uvarint(s.mem[destChunkDataLoc : destChunkDataLoc+binary.MaxVarintLen64])
	if read <= 0 {
		return fmt.Errorf("failed to read the chunk")
	}
	chunkMetaLoc := destChunkDataLoc + uint64(read)

	offset := uint64(0)
	di.curChunkHeader = make([]MetaData, int(numDocs))
	for i := 0; i < int(numDocs); i++ {
		di.curChunkHeader[i].DocNum, read = binary.Uvarint(s.mem[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
		offset += uint64(read)
		di.curChunkHeader[i].DocDvOffset, read = binary.Uvarint(s.mem[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
		offset += uint64(read)
	}

	compressedDataLoc := chunkMetaLoc + offset
	dataLength := curChunkEnd - compressedDataLoc
	di.curChunkData = s.mem[compressedDataLoc : compressedDataLoc+dataLength]
	di.curChunkNum = chunkNumber
	di.uncompressed = di.uncompressed[:0]
	return nil
}

func (di *docValueReader) visitDocValues(docNum uint64,
	visitor index.DocumentFieldTermVisitor) error {
	// binary search the term locations for the docNum
	start, end := di.getDocValueLocs(docNum)
	if start == math.MaxUint64 || end == math.MaxUint64 {
		return nil
	}

	var uncompressed []byte
	var err error
	// use the uncompressed copy if available
	if len(di.uncompressed) > 0 {
		uncompressed = di.uncompressed
	} else {
		// uncompress the already loaded data
		uncompressed, err = snappy.Decode(di.uncompressed[:cap(di.uncompressed)], di.curChunkData)
		if err != nil {
			return err
		}
		di.uncompressed = uncompressed
	}

	// pick the terms for the given docNum
	uncompressed = uncompressed[start:end]
	for {
		i := bytes.Index(uncompressed, termSeparatorSplitSlice)
		if i < 0 {
			break
		}

		visitor(di.field, uncompressed[0:i])
		uncompressed = uncompressed[i+1:]
	}

	return nil
}

func (di *docValueReader) getDocValueLocs(docNum uint64) (uint64, uint64) {
	i := sort.Search(len(di.curChunkHeader), func(i int) bool {
		return di.curChunkHeader[i].DocNum >= docNum
	})
	if i < len(di.curChunkHeader) && di.curChunkHeader[i].DocNum == docNum {
		return ReadDocValueBoundary(i, di.curChunkHeader)
	}
	return math.MaxUint64, math.MaxUint64
}

// VisitDocumentFieldTerms is an implementation of the
// DocumentFieldTermVisitable interface
func (s *SegmentBase) VisitDocumentFieldTerms(localDocNum uint64, fields []string,
	visitor index.DocumentFieldTermVisitor) error {
	fieldIDPlus1 := uint16(0)
	ok := true
	for _, field := range fields {
		if fieldIDPlus1, ok = s.fieldsMap[field]; !ok {
			continue
		}
		// find the chunkNumber where the docValues are stored
		docInChunk := localDocNum / uint64(s.chunkFactor)

		if dvIter, exists := s.fieldDvReaders[fieldIDPlus1-1]; exists &&
			dvIter != nil {
			// check if the chunk is already loaded
			if docInChunk != dvIter.curChunkNumber() {
				err := dvIter.loadDvChunk(docInChunk, localDocNum, s)
				if err != nil {
					continue
				}
			}

			_ = dvIter.visitDocValues(localDocNum, visitor)
		}
	}
	return nil
}

// VisitableDocValueFields returns the list of fields with
// persisted doc value terms ready to be visitable using the
// VisitDocumentFieldTerms method.
func (s *Segment) VisitableDocValueFields() ([]string, error) {
	rv := make([]string, 0, len(s.fieldDvReaders))
	for fieldID, field := range s.fieldsInv {
		if dvIter, ok := s.fieldDvReaders[uint16(fieldID)]; ok &&
			dvIter != nil {
			rv = append(rv, field)
		}
	}
	return rv, nil
}
