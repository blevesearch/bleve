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

	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
	"github.com/couchbaselabs/vellum"
	"github.com/golang/snappy"
)

const version uint32 = 1

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

	var dictLocs []uint64
	dictLocs, err = persistDictionary(memSegment, cr, postingsLocs)
	if err != nil {
		return err
	}

	var fieldIndexStart uint64
	fieldIndexStart, err = persistFields(memSegment, cr, dictLocs)
	if err != nil {
		return err
	}

	err = persistFooter(uint64(len(memSegment.Stored)), storedIndexOffset,
		fieldIndexStart, chunkFactor, cr)
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

		buf := make([]byte, binary.MaxVarintLen64)
		// write out the meta length
		n := binary.PutUvarint(buf, uint64(len(metaBytes)))
		_, err := w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
		// write out the compressed data length
		n = binary.PutUvarint(buf, uint64(len(compressed)))
		_, err = w.Write(buf[:n])
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
	for postingID := range memSegment.Postings {
		postingsListItr := memSegment.Postings[postingID].Iterator()

		total := uint64(len(memSegment.Stored))/uint64(chunkFactor) + 1

		var freqNormBuf []byte
		var offset int

		var encodingBuf bytes.Buffer
		encoder := govarint.NewU64Base128Encoder(&encodingBuf)

		chunkLens := make([]uint64, total)
		var currChunk uint64
		for postingsListItr.HasNext() {
			docNum := postingsListItr.Next()
			chunk := uint64(docNum) / uint64(chunkFactor)

			if chunk != currChunk {
				// starting a new chunk
				if encoder != nil {
					// close out last
					encoder.Close()
					encodingBytes := encodingBuf.Bytes()
					chunkLens[currChunk] = uint64(len(encodingBytes))
					freqNormBuf = append(freqNormBuf, encodingBytes...)
					encodingBuf.Reset()
					encoder = govarint.NewU64Base128Encoder(&encodingBuf)
				}

				currChunk = chunk
			}

			// put freq
			_, err := encoder.PutU64(memSegment.Freqs[postingID][offset])
			if err != nil {
				return nil, nil, err
			}

			// put norm
			norm := memSegment.Norms[postingID][offset]
			normBits := math.Float32bits(norm)
			_, err = encoder.PutU32(normBits)
			if err != nil {
				return nil, nil, err
			}

			offset++
		}

		// close out last chunk
		if encoder != nil {
			// fix me write freq/norms
			encoder.Close()
			encodingBytes := encodingBuf.Bytes()
			chunkLens[currChunk] = uint64(len(encodingBytes))
			freqNormBuf = append(freqNormBuf, encodingBytes...)
		}

		// record where this postings freq info starts
		freqOffsets = append(freqOffsets, uint64(w.Count()))

		buf := make([]byte, binary.MaxVarintLen64)
		// write out the number of chunks
		n := binary.PutUvarint(buf, uint64(total))
		_, err := w.Write(buf[:n])
		if err != nil {
			return nil, nil, err
		}
		// write out the chunk lens
		for _, chunkLen := range chunkLens {
			n := binary.PutUvarint(buf, uint64(chunkLen))
			_, err = w.Write(buf[:n])
			if err != nil {
				return nil, nil, err
			}
		}
		// write out the data
		_, err = w.Write(freqNormBuf)
		if err != nil {
			return nil, nil, err
		}

	}

	// now do it again for the locations
	for postingID := range memSegment.Postings {
		postingsListItr := memSegment.Postings[postingID].Iterator()

		total := uint64(len(memSegment.Stored))/uint64(chunkFactor) + 1

		var locBuf []byte
		var offset int
		var locOffset int

		var encodingBuf bytes.Buffer
		encoder := govarint.NewU64Base128Encoder(&encodingBuf)

		chunkLens := make([]uint64, total)
		var currChunk uint64
		for postingsListItr.HasNext() {
			docNum := postingsListItr.Next()
			chunk := uint64(docNum) / uint64(chunkFactor)

			if chunk != currChunk {
				// starting a new chunk
				if encoder != nil {
					// close out last
					encoder.Close()
					encodingBytes := encodingBuf.Bytes()
					chunkLens[currChunk] = uint64(len(encodingBytes))
					locBuf = append(locBuf, encodingBytes...)
					encodingBuf.Reset()
					encoder = govarint.NewU64Base128Encoder(&encodingBuf)
				}
				currChunk = chunk
			}

			for i := 0; i < int(memSegment.Freqs[postingID][offset]); i++ {

				if len(memSegment.Locfields[postingID]) > 0 {
					// put field
					_, err := encoder.PutU64(uint64(memSegment.Locfields[postingID][locOffset]))
					if err != nil {
						return nil, nil, err
					}

					// put pos
					_, err = encoder.PutU64(memSegment.Locpos[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put start
					_, err = encoder.PutU64(memSegment.Locstarts[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put end
					_, err = encoder.PutU64(memSegment.Locends[postingID][locOffset])
					if err != nil {
						return nil, nil, err
					}

					// put array positions
					num := len(memSegment.Locarraypos[postingID][locOffset])

					// put the number of array positions to follow
					_, err = encoder.PutU64(uint64(num))
					if err != nil {
						return nil, nil, err
					}

					// put each array position
					for j := 0; j < num; j++ {
						_, err = encoder.PutU64(memSegment.Locarraypos[postingID][locOffset][j])
						if err != nil {
							return nil, nil, err
						}
					}
				}

				locOffset++
			}
			offset++
		}

		// close out last chunk
		if encoder != nil {
			// fix me write freq/norms
			encoder.Close()
			encodingBytes := encodingBuf.Bytes()
			chunkLens[currChunk] = uint64(len(encodingBytes))
			locBuf = append(locBuf, encodingBytes...)
		}

		// record where this postings loc info starts
		locOfffsets = append(locOfffsets, uint64(w.Count()))

		buf := make([]byte, binary.MaxVarintLen64)
		// write out the number of chunks
		n := binary.PutUvarint(buf, uint64(total))
		_, err := w.Write(buf[:n])
		if err != nil {
			return nil, nil, err
		}
		// write out the chunk lens
		for _, chunkLen := range chunkLens {
			n := binary.PutUvarint(buf, uint64(chunkLen))
			_, err = w.Write(buf[:n])
			if err != nil {
				return nil, nil, err
			}
		}
		// write out the data
		_, err = w.Write(locBuf)
		if err != nil {
			return nil, nil, err
		}

	}
	return freqOffsets, locOfffsets, nil
}

func persistPostingsLocs(memSegment *mem.Segment, w *CountHashWriter) ([]uint64, error) {
	var rv []uint64

	var postingsBuf bytes.Buffer
	for postingID := range memSegment.PostingsLocs {
		if postingID != 0 {
			postingsBuf.Reset()
		}

		// record where we start this posting loc
		rv = append(rv, uint64(w.Count()))

		// write out postings locs to memory so we know the len
		postingsLocLen, err := memSegment.PostingsLocs[postingID].WriteTo(&postingsBuf)
		if err != nil {
			return nil, err
		}

		buf := make([]byte, binary.MaxVarintLen64)
		// write out the length of this postings locs
		n := binary.PutUvarint(buf, uint64(postingsLocLen))
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write out the postings list itself
		_, err = w.Write(postingsBuf.Bytes())
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func persistPostingsLists(memSegment *mem.Segment, w *CountHashWriter, postingsListLocs, freqOffsets, locOffsets []uint64) ([]uint64, error) {
	var rv []uint64

	var postingsBuf bytes.Buffer
	for postingID := range memSegment.Postings {
		if postingID != 0 {
			postingsBuf.Reset()
		}

		// record where we start this posting list
		rv = append(rv, uint64(w.Count()))

		// write out postings list to memory so we know the len
		postingsListLen, err := memSegment.Postings[postingID].WriteTo(&postingsBuf)
		if err != nil {
			return nil, err
		}

		// write out the start of the term info
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, freqOffsets[postingID])
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write out the start of the loc info
		n = binary.PutUvarint(buf, locOffsets[postingID])
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write out the start of the loc posting list
		n = binary.PutUvarint(buf, postingsListLocs[postingID])
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write out the length of this postings list
		n = binary.PutUvarint(buf, uint64(postingsListLen))
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write out the postings list itself
		_, err = w.Write(postingsBuf.Bytes())
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

func persistFields(memSegment *mem.Segment, w *CountHashWriter, dictLocs []uint64) (uint64, error) {
	var rv uint64

	var fieldStarts []uint64
	for fieldID, fieldName := range memSegment.FieldsInv {

		// record start of this field
		fieldStarts = append(fieldStarts, uint64(w.Count()))

		buf := make([]byte, binary.MaxVarintLen64)
		// write out dict location for this field
		n := binary.PutUvarint(buf, dictLocs[fieldID])
		_, err := w.Write(buf[:n])
		if err != nil {
			return 0, err
		}

		// write out the length of the field name
		n = binary.PutUvarint(buf, uint64(len(fieldName)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return 0, err
		}

		// write out the field name
		_, err = w.Write([]byte(fieldName))
		if err != nil {
			return 0, err
		}
	}

	// now write out the fields index
	rv = uint64(w.Count())

	// now write out the stored doc index
	for fieldID := range memSegment.FieldsInv {
		err := binary.Write(w, binary.BigEndian, fieldStarts[fieldID])
		if err != nil {
			return 0, err
		}
	}

	return rv, nil
}

// NOTE: update if you make the footer bigger
//               crc + ver + chunk + field offset + stored offset + num docs
const FooterSize = 4 + 4 + 4 + 8 + 8 + 8

func persistFooter(numDocs, storedIndexOffset, fieldIndexOffset uint64,
	chunkFactor uint32, w *CountHashWriter) error {
	// write out the number of docs
	err := binary.Write(w, binary.BigEndian, numDocs)
	if err != nil {
		return err
	}
	// write out the stored field index location:
	err = binary.Write(w, binary.BigEndian, storedIndexOffset)
	if err != nil {
		return err
	}
	// write out the field index location
	err = binary.Write(w, binary.BigEndian, fieldIndexOffset)
	if err != nil {
		return err
	}
	// write out 32-bit chunk factor
	err = binary.Write(w, binary.BigEndian, chunkFactor)
	if err != nil {
		return err
	}
	// write out 32-bit version
	err = binary.Write(w, binary.BigEndian, version)
	if err != nil {
		return err
	}
	// write out CRC-32 of everything upto but not including this CRC
	err = binary.Write(w, binary.BigEndian, w.Sum32())
	if err != nil {
		return err
	}
	return nil
}
