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
	"io"
	"reflect"

	"github.com/golang/snappy"
)

var reflectStaticSizeMetaData int

func init() {
	var md MetaData
	reflectStaticSizeMetaData = int(reflect.TypeOf(md).Size())
}

var termSeparator byte = 0xff
var termSeparatorSplitSlice = []byte{termSeparator}

type chunkedContentCoder struct {
	final        []byte
	chunkSize    uint64
	currChunk    uint64
	chunkLens    []uint64
	chunkMetaBuf bytes.Buffer
	chunkBuf     bytes.Buffer

	chunkMeta []MetaData
}

// MetaData represents the data information inside a
// chunk.
type MetaData struct {
	DocNum      uint64 // docNum of the data inside the chunk
	DocDvOffset uint64 // offset of data inside the chunk for the given docid
}

// newChunkedContentCoder returns a new chunk content coder which
// packs data into chunks based on the provided chunkSize
func newChunkedContentCoder(chunkSize uint64,
	maxDocNum uint64) *chunkedContentCoder {
	total := maxDocNum/chunkSize + 1
	rv := &chunkedContentCoder{
		chunkSize: chunkSize,
		chunkLens: make([]uint64, total),
		chunkMeta: make([]MetaData, 0, total),
	}

	return rv
}

// Reset lets you reuse this chunked content coder. Buffers are reset
// and re used. You cannot change the chunk size.
func (c *chunkedContentCoder) Reset() {
	c.currChunk = 0
	c.final = c.final[:0]
	c.chunkBuf.Reset()
	c.chunkMetaBuf.Reset()
	for i := range c.chunkLens {
		c.chunkLens[i] = 0
	}
	c.chunkMeta = c.chunkMeta[:0]
}

// Close indicates you are done calling Add() this allows
// the final chunk to be encoded.
func (c *chunkedContentCoder) Close() error {
	return c.flushContents()
}

func (c *chunkedContentCoder) flushContents() error {
	// flush the contents, with meta information at first
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(c.chunkMeta)))
	_, err := c.chunkMetaBuf.Write(buf[:n])
	if err != nil {
		return err
	}

	// convert the document data lens to data offsets
	if len(c.chunkMeta) > 1 {
		var runningOffset uint64
		var index, i int
		for i = 1; i < len(c.chunkMeta); i++ {
			runningOffset += c.chunkMeta[i-1].DocDvOffset
			c.chunkMeta[index].DocDvOffset = runningOffset
			index++
		}
		c.chunkMeta[index].DocDvOffset = c.chunkMeta[i-1].DocDvOffset
	}

	// write out the metaData slice
	for _, meta := range c.chunkMeta {
		_, err := writeUvarints(&c.chunkMetaBuf, meta.DocNum, meta.DocDvOffset)
		if err != nil {
			return err
		}
	}

	// write the metadata to final data
	metaData := c.chunkMetaBuf.Bytes()
	c.final = append(c.final, c.chunkMetaBuf.Bytes()...)
	// write the compressed data to the final data
	compressedData := snappy.Encode(nil, c.chunkBuf.Bytes())
	c.final = append(c.final, compressedData...)

	c.chunkLens[c.currChunk] = uint64(len(compressedData) + len(metaData))
	return nil
}

// Add encodes the provided byte slice into the correct chunk for the provided
// doc num.  You MUST call Add() with increasing docNums.
func (c *chunkedContentCoder) Add(docNum uint64, vals []byte) error {
	chunk := docNum / c.chunkSize
	if chunk != c.currChunk {
		// flush out the previous chunk details
		err := c.flushContents()
		if err != nil {
			return err
		}
		// clearing the chunk specific meta for next chunk
		c.chunkBuf.Reset()
		c.chunkMetaBuf.Reset()
		c.chunkMeta = c.chunkMeta[:0]
		c.currChunk = chunk
	}

	// mark the data size for this doc
	dvSize, err := c.chunkBuf.Write(vals)
	if err != nil {
		return err
	}

	c.chunkMeta = append(c.chunkMeta, MetaData{
		DocNum:      docNum,
		DocDvOffset: uint64(dvSize),
	})
	return nil
}

// Write commits all the encoded chunked contents to the provided writer.
func (c *chunkedContentCoder) Write(w io.Writer) (int, error) {
	var tw int
	buf := make([]byte, binary.MaxVarintLen64)
	// write out the number of chunks
	n := binary.PutUvarint(buf, uint64(len(c.chunkLens)))
	nw, err := w.Write(buf[:n])
	tw += nw
	if err != nil {
		return tw, err
	}
	// write out the chunk lens
	for _, chunkLen := range c.chunkLens {
		n := binary.PutUvarint(buf, uint64(chunkLen))
		nw, err = w.Write(buf[:n])
		tw += nw
		if err != nil {
			return tw, err
		}
	}
	// write out the data
	nw, err = w.Write(c.final)
	tw += nw
	if err != nil {
		return tw, err
	}
	return tw, nil
}

// ReadDocValueBoundary elicits the start, end offsets from a
// starting offset based metaData header slice
func ReadDocValueBoundary(chunk int, metaHeaders []MetaData) (uint64, uint64) {
	var start, end uint64
	if chunk > len(metaHeaders) {
		return start, end
	}

	if chunk > 0 {
		start = metaHeaders[chunk-1].DocDvOffset
	}

	if chunk < len(metaHeaders)-1 {
		end = metaHeaders[chunk].DocDvOffset
	} else {
		end = start + metaHeaders[chunk].DocDvOffset
	}

	return start, end
}
