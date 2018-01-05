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
	"bufio"
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"sort"

	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
	"github.com/couchbase/vellum"
	"github.com/golang/snappy"
)

const version uint32 = 2

const fieldNotUninverted = math.MaxUint64

// PersistSegment takes the in-memory segment and persists it to the specified
// path in the zap file format.
func PersistSegment(memSegment *mem.Segment, path string, chunkFactor uint32) (err error) {

	flag := os.O_RDWR | os.O_CREATE

	f, err := os.OpenFile(path, flag, 0600)
	if err != nil {
		return err
	}

	// bufer the output
	br := bufio.NewWriter(f)

	// wrap it for counting (tracking offsets)
	cr := NewCountHashWriter(br)

	var storedIndexOffset uint64
	var dictLocs []uint64
	docValueOffset := uint64(fieldNotUninverted)
	if len(memSegment.Stored) > 0 {

		storedIndexOffset, err = persistStored(memSegment, cr)
		if err != nil {
			return err
		}

		var freqOffsets, locOffsets []uint64
		freqOffsets, locOffsets, err = persistPostingDetails(memSegment, cr, chunkFactor)
		if err != nil {
			return err
		}

		var postingsListLocs []uint64
		postingsListLocs, err = persistPostingsLocs(memSegment, cr)
		if err != nil {
			return err
		}

		var postingsLocs []uint64
		postingsLocs, err = persistPostingsLists(memSegment, cr, postingsListLocs, freqOffsets, locOffsets)
		if err != nil {
			return err
		}

		dictLocs, err = persistDictionary(memSegment, cr, postingsLocs)
		if err != nil {
			return err
		}

		docValueOffset, err = persistFieldDocValues(cr, chunkFactor, memSegment)
		if err != nil {
			return err
		}

	} else {
		dictLocs = make([]uint64, len(memSegment.FieldsInv))
	}

	var fieldIndexStart uint64
	fieldIndexStart, err = persistFields(memSegment.FieldsInv, cr, dictLocs)
	if err != nil {
		return err
	}

	err = persistFooter(uint64(len(memSegment.Stored)), storedIndexOffset,
		fieldIndexStart, docValueOffset, chunkFactor, cr)
	if err != nil {
		return err
	}

	err = br.Flush()
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	return nil
}

func persistStored(memSegment *mem.Segment, w *CountHashWriter) (uint64, error) {

	var curr int
	var metaBuf bytes.Buffer
	var data, compressed []byte

	docNumOffsets := make(map[int]uint64, len(memSegment.Stored))

	for docNum, storedValues := range memSegment.Stored {
		if docNum != 0 {
			// reset buffer if necessary
			metaBuf.Reset()
			data = data[:0]
			compressed = compressed[:0]
			curr = 0
		}

		metaEncoder := govarint.NewU64Base128Encoder(&metaBuf)

		// encode fields in order
		for fieldID := range memSegment.FieldsInv {
			if storedFieldValues, ok := storedValues[uint16(fieldID)]; ok {
				// has stored values for this field
				num := len(storedFieldValues)

				// process each value
				for i := 0; i < num; i++ {
					// encode field
					_, err2 := metaEncoder.PutU64(uint64(fieldID))
					if err2 != nil {
						return 0, err2
					}
					// encode type
					_, err2 = metaEncoder.PutU64(uint64(memSegment.StoredTypes[docNum][uint16(fieldID)][i]))
					if err2 != nil {
						return 0, err2
					}
					// encode start offset
					_, err2 = metaEncoder.PutU64(uint64(curr))
					if err2 != nil {
						return 0, err2
					}
					// end len
					_, err2 = metaEncoder.PutU64(uint64(len(storedFieldValues[i])))
					if err2 != nil {
						return 0, err2
					}
					// encode number of array pos
					_, err2 = metaEncoder.PutU64(uint64(len(memSegment.StoredPos[docNum][uint16(fieldID)][i])))
					if err2 != nil {
						return 0, err2
					}
					// encode all array positions
					for j := 0; j < len(memSegment.StoredPos[docNum][uint16(fieldID)][i]); j++ {
						_, err2 = metaEncoder.PutU64(memSegment.StoredPos[docNum][uint16(fieldID)][i][j])
						if err2 != nil {
							return 0, err2
						}
					}
					// append data
					data = append(data, storedFieldValues[i]...)
					// update curr
					curr += len(storedFieldValues[i])
				}
			}
		}
		metaEncoder.Close()

		metaBytes := metaBuf.Bytes()

		// compress the data
		compressed = snappy.Encode(compressed, data)

		// record where we're about to start writing
		docNumOffsets[docNum] = uint64(w.Count())

		// write out the meta len and compressed data len
		_, err := writeUvarints(w, uint64(len(metaBytes)), uint64(len(compressed)))
		if err != nil {
			return 0, err
		}

		// now write the meta
		_, err = w.Write(metaBytes)
		if err != nil {
			return 0, err
		}
		// now write the compressed data
		_, err = w.Write(compressed)
		if err != nil {
			return 0, err
		}
	}

	// return value is the start of the stored index
	rv := uint64(w.Count())
	// now write out the stored doc index
	for docNum := range memSegment.Stored {
		err := binary.Write(w, binary.BigEndian, docNumOffsets[docNum])
		if err != nil {
			return 0, err
		}
	}

	return rv, nil
}

func persistPostingDetails(memSegment *mem.Segment, w *CountHashWriter, chunkFactor uint32) ([]uint64, []uint64, error) {
	var freqOffsets, locOfffsets []uint64
	tfEncoder := newChunkedIntCoder(uint64(chunkFactor), uint64(len(memSegment.Stored)-1))
	for postingID := range memSegment.Postings {
		if postingID != 0 {
			tfEncoder.Reset()
		}
		postingsListItr := memSegment.Postings[postingID].Iterator()
		var offset int
		for postingsListItr.HasNext() {

			docNum := uint64(postingsListItr.Next())

			// put freq
			err := tfEncoder.Add(docNum, memSegment.Freqs[postingID][offset])
			if err != nil {
				return nil, nil, err
			}

			// put norm
			norm := memSegment.Norms[postingID][offset]
			normBits := math.Float32bits(norm)
			err = tfEncoder.Add(docNum, uint64(normBits))
			if err != nil {
				return nil, nil, err
			}

			offset++
		}

		// record where this postings freq info starts
		freqOffsets = append(freqOffsets, uint64(w.Count()))

		tfEncoder.Close()
		_, err := tfEncoder.Write(w)
		if err != nil {
			return nil, nil, err
		}

	}

	// now do it again for the locations
	locEncoder := newChunkedIntCoder(uint64(chunkFactor), uint64(len(memSegment.Stored)-1))
	for postingID := range memSegment.Postings {
		if postingID != 0 {
			locEncoder.Reset()
		}
		postingsListItr := memSegment.Postings[postingID].Iterator()
		var offset int
		var locOffset int
		for postingsListItr.HasNext() {
			docNum := uint64(postingsListItr.Next())
			for i := 0; i < int(memSegment.Freqs[postingID][offset]); i++ {
				if len(memSegment.Locfields[postingID]) > 0 {
					// put field
					err := locEncoder.Add(docNum, uint64(memSegment.Locfields[postingID][locOffset]))
					if err != nil {
						return nil, nil, err
					}

					// put pos

					err = locEncoder.Add(docNum, memSegment.Locpos[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put start
					err = locEncoder.Add(docNum, memSegment.Locstarts[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put end
					err = locEncoder.Add(docNum, memSegment.Locends[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put array positions
					num := len(memSegment.Locarraypos[postingID][locOffset])

					// put the number of array positions to follow
					err = locEncoder.Add(docNum, uint64(num))
					if err != nil {
						return nil, nil, err
					}

					// put each array position
					for j := 0; j < num; j++ {
						err = locEncoder.Add(docNum, memSegment.Locarraypos[postingID][locOffset][j])
						if err != nil {
							return nil, nil, err
						}
					}
				}
				locOffset++
			}
			offset++
		}

		// record where this postings loc info starts
		locOfffsets = append(locOfffsets, uint64(w.Count()))
		locEncoder.Close()
		_, err := locEncoder.Write(w)
		if err != nil {
			return nil, nil, err
		}
	}
	return freqOffsets, locOfffsets, nil
}

func persistPostingsLocs(memSegment *mem.Segment, w *CountHashWriter) (rv []uint64, err error) {
	for postingID := range memSegment.PostingsLocs {
		// record where we start this posting loc
		rv = append(rv, uint64(w.Count()))
		// write out the length and bitmap
		_, err = writeRoaringWithLen(memSegment.PostingsLocs[postingID], w)
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func persistPostingsLists(memSegment *mem.Segment, w *CountHashWriter,
	postingsListLocs, freqOffsets, locOffsets []uint64) (rv []uint64, err error) {
	for postingID := range memSegment.Postings {
		// record where we start this posting list
		rv = append(rv, uint64(w.Count()))

		// write out the term info, loc info, and loc posting list offset
		_, err = writeUvarints(w, freqOffsets[postingID],
			locOffsets[postingID], postingsListLocs[postingID])
		if err != nil {
			return nil, err
		}

		// write out the length and bitmap
		_, err = writeRoaringWithLen(memSegment.Postings[postingID], w)
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func persistDictionary(memSegment *mem.Segment, w *CountHashWriter, postingsLocs []uint64) ([]uint64, error) {
	var rv []uint64

	var buffer bytes.Buffer
	for fieldID, fieldTerms := range memSegment.DictKeys {
		if fieldID != 0 {
			buffer.Reset()
		}

		// start a new vellum for this field
		builder, err := vellum.New(&buffer, nil)
		if err != nil {
			return nil, err
		}

		dict := memSegment.Dicts[fieldID]
		// now walk the dictionary in order of fieldTerms (already sorted)
		for i := range fieldTerms {
			postingID := dict[fieldTerms[i]] - 1
			postingsAddr := postingsLocs[postingID]
			err = builder.Insert([]byte(fieldTerms[i]), postingsAddr)
			if err != nil {
				return nil, err
			}
		}
		err = builder.Close()
		if err != nil {
			return nil, err
		}

		// record where this dictionary starts
		rv = append(rv, uint64(w.Count()))

		vellumData := buffer.Bytes()

		// write out the length of the vellum data
		buf := make([]byte, binary.MaxVarintLen64)
		// write out the number of chunks
		n := binary.PutUvarint(buf, uint64(len(vellumData)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

type docIDRange []uint64

func (a docIDRange) Len() int           { return len(a) }
func (a docIDRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a docIDRange) Less(i, j int) bool { return a[i] < a[j] }

func persistDocValues(memSegment *mem.Segment, w *CountHashWriter,
	chunkFactor uint32) (map[uint16]uint64, error) {
	fieldChunkOffsets := make(map[uint16]uint64, len(memSegment.FieldsInv))
	fdvEncoder := newChunkedContentCoder(uint64(chunkFactor), uint64(len(memSegment.Stored)-1))

	for fieldID := range memSegment.DocValueFields {
		field := memSegment.FieldsInv[fieldID]
		docTermMap := make(map[uint64][]byte, 0)
		dict, err := memSegment.Dictionary(field)
		if err != nil {
			return nil, err
		}

		dictItr := dict.Iterator()
		next, err := dictItr.Next()
		for err == nil && next != nil {
			postings, err1 := dict.PostingsList(next.Term, nil)
			if err1 != nil {
				return nil, err
			}

			postingsItr := postings.Iterator()
			nextPosting, err2 := postingsItr.Next()
			for err2 == nil && nextPosting != nil {
				docNum := nextPosting.Number()
				docTermMap[docNum] = append(docTermMap[docNum], []byte(next.Term)...)
				docTermMap[docNum] = append(docTermMap[docNum], termSeparator)
				nextPosting, err2 = postingsItr.Next()
			}
			if err2 != nil {
				return nil, err2
			}

			next, err = dictItr.Next()
		}

		if err != nil {
			return nil, err
		}
		// sort wrt to docIDs
		var docNumbers docIDRange
		for k := range docTermMap {
			docNumbers = append(docNumbers, k)
		}
		sort.Sort(docNumbers)

		for _, docNum := range docNumbers {
			err = fdvEncoder.Add(docNum, docTermMap[docNum])
			if err != nil {
				return nil, err
			}
		}

		fieldChunkOffsets[fieldID] = uint64(w.Count())
		err = fdvEncoder.Close()
		if err != nil {
			return nil, err
		}
		// persist the doc value details for this field
		_, err = fdvEncoder.Write(w)
		if err != nil {
			return nil, err
		}
		// resetting encoder for the next field
		fdvEncoder.Reset()
	}

	return fieldChunkOffsets, nil
}

func persistFieldDocValues(w *CountHashWriter, chunkFactor uint32,
	memSegment *mem.Segment) (uint64, error) {

	fieldDvOffsets, err := persistDocValues(memSegment, w, chunkFactor)
	if err != nil {
		return 0, err
	}

	fieldDocValuesOffset := uint64(w.Count())
	buf := make([]byte, binary.MaxVarintLen64)
	offset := uint64(0)
	ok := true
	for fieldID := range memSegment.FieldsInv {
		// if the field isn't configured for docValue, then mark
		// the offset accordingly
		if offset, ok = fieldDvOffsets[uint16(fieldID)]; !ok {
			offset = fieldNotUninverted
		}
		n := binary.PutUvarint(buf, uint64(offset))
		_, err := w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
	}

	return fieldDocValuesOffset, nil
}
