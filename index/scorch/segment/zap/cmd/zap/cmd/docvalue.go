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

package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"

	"github.com/blevesearch/bleve/index/scorch/segment/zap"
	"github.com/golang/snappy"
	"github.com/spf13/cobra"
)

// docvalueCmd represents the docvalue command
var docvalueCmd = &cobra.Command{
	Use:   "docvalue [path] <field> optional <docNum> optional",
	Short: "docvalue prints the docvalue details by field, and docNum",
	Long:  `The docvalue command lets you explore the docValues in order of field and by doc number.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("must specify index file path")
		}

		data := segment.Data()
		crcOffset := len(data) - 4
		verOffset := crcOffset - 4
		chunkOffset := verOffset - 4
		fieldsOffset := chunkOffset - 16
		fieldsIndexOffset := binary.BigEndian.Uint64(data[fieldsOffset : fieldsOffset+8])
		fieldsIndexEnd := uint64(len(data) - zap.FooterSize)

		// iterate through fields index
		var fieldInv []string
		var id, read, fieldLoc uint64
		var nread int
		for fieldsIndexOffset+(8*id) < fieldsIndexEnd {
			addr := binary.BigEndian.Uint64(data[fieldsIndexOffset+(8*id) : fieldsIndexOffset+(8*id)+8])
			var n uint64
			_, read := binary.Uvarint(data[addr+n : fieldsIndexEnd])
			n += uint64(read)

			var nameLen uint64
			nameLen, read = binary.Uvarint(data[addr+n : fieldsIndexEnd])
			n += uint64(read)

			name := string(data[addr+n : addr+n+nameLen])

			id++
			fieldInv = append(fieldInv, name)
		}

		dvLoc := segment.DocValueOffset()
		fieldDvLoc := uint64(0)
		var fieldName string
		var fieldID uint16

		// if no fields are specified then print the docValue offsets for all fields set
		for id, field := range fieldInv {
			fieldLoc, nread = binary.Uvarint(data[dvLoc+read : dvLoc+read+binary.MaxVarintLen64])
			if nread <= 0 {
				return fmt.Errorf("loadDvIterators: failed to read the docvalue offsets for field %d", fieldID)
			}
			read += uint64(nread)
			if len(args) == 1 {
				// if no field args are given, then print out the dv locations for all fields
				fmt.Printf("fieldID: %d '%s' docvalue at %d (%x)\n", id, field, fieldLoc, fieldLoc)
				continue
			}

			if field != args[1] {
				continue
			} else {
				fieldDvLoc = fieldLoc
				fieldName = field
				fieldID = uint16(id)
			}

		}

		// done with the fields dv locs printing for the given zap file
		if len(args) == 1 {
			return nil
		}

		if fieldName == "" || fieldDvLoc == 0 {
			return fmt.Errorf("no field found for given field arg: %s", args[1])
		}

		// read the number of chunks
		var offset, clen, numChunks uint64
		numChunks, nread = binary.Uvarint(data[fieldDvLoc : fieldDvLoc+binary.MaxVarintLen64])
		if nread <= 0 {
			return fmt.Errorf("failed to read the field "+
				"doc values for field %s", fieldName)
		}
		offset += uint64(nread)

		if len(args) == 2 {
			fmt.Printf("number of chunks: %d\n", numChunks)
		}

		// read the length of chunks
		chunkLens := make([]uint64, numChunks)
		for i := 0; i < int(numChunks); i++ {
			clen, nread = binary.Uvarint(data[fieldDvLoc+offset : fieldDvLoc+offset+binary.MaxVarintLen64])
			if nread <= 0 {
				return fmt.Errorf("corrupted chunk length for chunk number: %d", i)
			}

			chunkLens[i] = clen
			offset += uint64(nread)
			if len(args) == 2 {
				fmt.Printf("chunk: %d size: %d \n", i, clen)
			}
		}

		if len(args) == 2 {
			return nil
		}

		localDocNum, err := strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("unable to parse doc number: %v", err)
		}

		if localDocNum >= int(segment.NumDocs()) {
			return fmt.Errorf("invalid doc number %d (valid 0 - %d)", localDocNum, segment.NumDocs()-1)
		}

		// find the chunkNumber where the docValues are stored
		docInChunk := uint64(localDocNum) / uint64(segment.ChunkFactor())

		if numChunks < docInChunk {
			return fmt.Errorf("no chunk exists for chunk number: %d for docID: %d", docInChunk, localDocNum)
		}

		destChunkDataLoc := fieldDvLoc + offset
		for i := 0; i < int(docInChunk); i++ {
			destChunkDataLoc += chunkLens[i]
		}
		curChunkSize := chunkLens[docInChunk]

		// read the number of docs reside in the chunk
		numDocs := uint64(0)
		numDocs, nread = binary.Uvarint(data[destChunkDataLoc : destChunkDataLoc+binary.MaxVarintLen64])
		if nread <= 0 {
			return fmt.Errorf("failed to read the target chunk: %d", docInChunk)
		}
		chunkMetaLoc := destChunkDataLoc + uint64(nread)

		offset = uint64(0)
		curChunkHeader := make([]zap.MetaData, int(numDocs))
		for i := 0; i < int(numDocs); i++ {
			curChunkHeader[i].DocID, nread = binary.Uvarint(data[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
			offset += uint64(nread)
			curChunkHeader[i].DocDvLoc, nread = binary.Uvarint(data[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
			offset += uint64(nread)
			curChunkHeader[i].DocDvLen, nread = binary.Uvarint(data[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
			offset += uint64(nread)
		}

		compressedDataLoc := chunkMetaLoc + offset
		dataLength := destChunkDataLoc + curChunkSize - compressedDataLoc
		curChunkData := data[compressedDataLoc : compressedDataLoc+dataLength]

		start, length := getDocValueLocs(uint64(localDocNum), curChunkHeader)
		if start == math.MaxUint64 || length == math.MaxUint64 {
			return nil
		}
		// uncompress the already loaded data
		uncompressed, err := snappy.Decode(nil, curChunkData)
		if err != nil {
			log.Printf("snappy err %+v ", err)
			return err
		}

		var termSeparator byte = 0xff
		var termSeparatorSplitSlice = []byte{termSeparator}
		// pick the terms for the given docID
		uncompressed = uncompressed[start : start+length]
		for {
			i := bytes.Index(uncompressed, termSeparatorSplitSlice)
			if i < 0 {
				break
			}

			fmt.Printf(" %s ", uncompressed[0:i])
			uncompressed = uncompressed[i+1:]
		}
		fmt.Printf(" \n ")
		return nil
	},
}

func getDocValueLocs(docID uint64, metaHeader []zap.MetaData) (uint64, uint64) {
	i := sort.Search(len(metaHeader), func(i int) bool {
		return metaHeader[i].DocID >= docID
	})
	if i < len(metaHeader) && metaHeader[i].DocID == docID {
		return metaHeader[i].DocDvLoc, metaHeader[i].DocDvLen
	}
	return math.MaxUint64, math.MaxUint64
}

func init() {
	RootCmd.AddCommand(docvalueCmd)
}
