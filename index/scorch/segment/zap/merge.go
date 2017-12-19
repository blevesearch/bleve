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
	"fmt"
	"math"
	"os"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/couchbase/vellum"
	"github.com/golang/snappy"
)

// Merge takes a slice of zap segments, bit masks describing which documents
// from the may be dropped, and creates a new segment containing the remaining
// data.  This new segment is built at the specified path, with the provided
// chunkFactor.
func Merge(segments []*Segment, drops []*roaring.Bitmap, path string,
	chunkFactor uint32) ([][]uint64, error) {
	flag := os.O_RDWR | os.O_CREATE

	f, err := os.OpenFile(path, flag, 0600)
	if err != nil {
		return nil, err
	}

	// bufer the output
	br := bufio.NewWriter(f)

	// wrap it for counting (tracking offsets)
	cr := NewCountHashWriter(br)

	fieldsInv := mergeFields(segments)
	fieldsMap := mapFields(fieldsInv)
	newSegDocCount := computeNewDocCount(segments, drops)

	var newDocNums [][]uint64
	var storedIndexOffset uint64
	var dictLocs []uint64
	if newSegDocCount > 0 {
		storedIndexOffset, newDocNums, err = mergeStoredAndRemap(segments, drops,
			fieldsMap, fieldsInv, newSegDocCount, cr)
		if err != nil {
			return nil, err
		}

		dictLocs, err = persistMergedRest(segments, drops, fieldsInv, fieldsMap,
			newDocNums, newSegDocCount, chunkFactor, cr)
		if err != nil {
			return nil, err
		}
	} else {
		dictLocs = make([]uint64, len(fieldsInv))
	}

	var fieldsIndexOffset uint64
	fieldsIndexOffset, err = persistFields(fieldsInv, cr, dictLocs)
	if err != nil {
		return nil, err
	}

	err = persistFooter(newSegDocCount, storedIndexOffset,
		fieldsIndexOffset, chunkFactor, cr)
	if err != nil {
		return nil, err
	}

	err = br.Flush()
	if err != nil {
		return nil, err
	}

	err = f.Sync()
	if err != nil {
		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	return newDocNums, nil
}

// mapFields takes the fieldsInv list and builds the map
func mapFields(fields []string) map[string]uint16 {
	rv := make(map[string]uint16)
	for i, fieldName := range fields {
		rv[fieldName] = uint16(i)
	}
	return rv
}

// computeNewDocCount determines how many documents will be in the newly
// merged segment when obsoleted docs are dropped
func computeNewDocCount(segments []*Segment, drops []*roaring.Bitmap) uint64 {
	var newSegDocCount uint64
	for segI, segment := range segments {
		segIAfterDrop := segment.NumDocs()
		if drops[segI] != nil {
			segIAfterDrop -= drops[segI].GetCardinality()
		}
		newSegDocCount += segIAfterDrop
	}
	return newSegDocCount
}

func persistMergedRest(segments []*Segment, drops []*roaring.Bitmap,
	fieldsInv []string, fieldsMap map[string]uint16, newDocNums [][]uint64,
	newSegDocCount uint64, chunkFactor uint32,
	w *CountHashWriter) ([]uint64, error) {

	rv := make([]uint64, len(fieldsInv))

	var vellumBuf bytes.Buffer
	// for each field
	for fieldID, fieldName := range fieldsInv {
		if fieldID != 0 {
			vellumBuf.Reset()
		}
		newVellum, err := vellum.New(&vellumBuf, nil)
		if err != nil {
			return nil, err
		}

		// collect FTS iterators from all segments for this field
		var dicts []*Dictionary
		var itrs []vellum.Iterator
		for _, segment := range segments {
			dict, err2 := segment.dictionary(fieldName)
			if err2 != nil {
				return nil, err2
			}
			dicts = append(dicts, dict)

			if dict != nil && dict.fst != nil {
				itr, err2 := dict.fst.Iterator(nil, nil)
				if err2 != nil && err2 != vellum.ErrIteratorDone {
					return nil, err2
				}
				if itr != nil {
					itrs = append(itrs, itr)
				}
			}
		}

		// create merging iterator
		mergeItr, err := vellum.NewMergeIterator(itrs, func(postingOffsets []uint64) uint64 {
			// we don't actually use the merged value
			return 0
		})

		tfEncoder := newChunkedIntCoder(uint64(chunkFactor), newSegDocCount-1)
		locEncoder := newChunkedIntCoder(uint64(chunkFactor), newSegDocCount-1)
		for err == nil {
			term, _ := mergeItr.Current()

			newRoaring := roaring.NewBitmap()
			newRoaringLocs := roaring.NewBitmap()
			tfEncoder.Reset()
			locEncoder.Reset()

			// now go back and get posting list for this term
			// but pass in the deleted docs for that segment
			for dictI, dict := range dicts {
				if dict == nil {
					continue
				}
				postings, err2 := dict.postingsList(string(term), drops[dictI])
				if err2 != nil {
					return nil, err2
				}

				postItr := postings.Iterator()
				next, err2 := postItr.Next()
				for next != nil && err2 == nil {
					hitNewDocNum := newDocNums[dictI][next.Number()]
					if hitNewDocNum == docDropped {
						return nil, fmt.Errorf("see hit with dropped doc num")
					}
					newRoaring.Add(uint32(hitNewDocNum))
					// encode norm bits
					norm := next.Norm()
					normBits := math.Float32bits(float32(norm))
					err3 := tfEncoder.Add(hitNewDocNum, next.Frequency(), uint64(normBits))
					if err3 != nil {
						return nil, err3
					}
					locs := next.Locations()
					if len(locs) > 0 {
						newRoaringLocs.Add(uint32(hitNewDocNum))
						for _, loc := range locs {
							args := make([]uint64, 0, 5+len(loc.ArrayPositions()))
							args = append(args, uint64(fieldsMap[loc.Field()]))
							args = append(args, loc.Pos())
							args = append(args, loc.Start())
							args = append(args, loc.End())
							args = append(args, uint64(len(loc.ArrayPositions())))
							args = append(args, loc.ArrayPositions()...)
							err = locEncoder.Add(hitNewDocNum, args...)
							if err != nil {
								return nil, err
							}
						}
					}
					next, err2 = postItr.Next()
				}
				if err != nil {
					return nil, err
				}

			}
			tfEncoder.Close()
			locEncoder.Close()

			if newRoaring.GetCardinality() > 0 {
				// this field/term actually has hits in the new segment, lets write it down
				freqOffset := uint64(w.Count())
				_, err = tfEncoder.Write(w)
				if err != nil {
					return nil, err
				}
				locOffset := uint64(w.Count())
				_, err = locEncoder.Write(w)
				if err != nil {
					return nil, err
				}
				postingLocOffset := uint64(w.Count())
				_, err = writeRoaringWithLen(newRoaringLocs, w)
				if err != nil {
					return nil, err
				}
				postingOffset := uint64(w.Count())
				// write out the start of the term info
				buf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(buf, freqOffset)
				_, err = w.Write(buf[:n])
				if err != nil {
					return nil, err
				}

				// write out the start of the loc info
				n = binary.PutUvarint(buf, locOffset)
				_, err = w.Write(buf[:n])
				if err != nil {
					return nil, err
				}

				// write out the start of the loc posting list
				n = binary.PutUvarint(buf, postingLocOffset)
				_, err = w.Write(buf[:n])
				if err != nil {
					return nil, err
				}
				_, err = writeRoaringWithLen(newRoaring, w)
				if err != nil {
					return nil, err
				}

				err = newVellum.Insert(term, postingOffset)
				if err != nil {
					return nil, err
				}
			}

			err = mergeItr.Next()
		}
		if err != nil && err != vellum.ErrIteratorDone {
			return nil, err
		}

		dictOffset := uint64(w.Count())
		err = newVellum.Close()
		if err != nil {
			return nil, err
		}
		vellumData := vellumBuf.Bytes()

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

		rv[fieldID] = dictOffset
	}

	return rv, nil
}

const docDropped = math.MaxUint64

func mergeStoredAndRemap(segments []*Segment, drops []*roaring.Bitmap,
	fieldsMap map[string]uint16, fieldsInv []string, newSegDocCount uint64,
	w *CountHashWriter) (uint64, [][]uint64, error) {
	var rv [][]uint64
	var newDocNum int

	var curr int
	var metaBuf bytes.Buffer
	var data, compressed []byte

	docNumOffsets := make([]uint64, newSegDocCount)

	// for each segment
	for segI, segment := range segments {
		var segNewDocNums []uint64

		// for each doc num
		for docNum := uint64(0); docNum < segment.numDocs; docNum++ {
			metaBuf.Reset()
			data = data[:0]
			compressed = compressed[:0]
			curr = 0

			metaEncoder := govarint.NewU64Base128Encoder(&metaBuf)

			if drops[segI] != nil && drops[segI].Contains(uint32(docNum)) {
				segNewDocNums = append(segNewDocNums, docDropped)
			} else {
				segNewDocNums = append(segNewDocNums, uint64(newDocNum))
				// collect all the data
				vals := make(map[uint16][][]byte)
				typs := make(map[uint16][]byte)
				poss := make(map[uint16][][]uint64)
				err := segment.VisitDocument(docNum, func(field string, typ byte, value []byte, pos []uint64) bool {
					fieldID := fieldsMap[field]
					vals[fieldID] = append(vals[fieldID], value)
					typs[fieldID] = append(typs[fieldID], typ)
					poss[fieldID] = append(poss[fieldID], pos)
					return true
				})
				if err != nil {
					return 0, nil, err
				}

				// now walk the fields in order
				for fieldID := range fieldsInv {

					if storedFieldValues, ok := vals[uint16(fieldID)]; ok {

						// has stored values for this field
						num := len(storedFieldValues)

						// process each value
						for i := 0; i < num; i++ {
							// encode field
							_, err2 := metaEncoder.PutU64(uint64(fieldID))
							if err2 != nil {
								return 0, nil, err2
							}
							// encode type
							_, err2 = metaEncoder.PutU64(uint64(typs[uint16(fieldID)][i]))
							if err2 != nil {
								return 0, nil, err2
							}
							// encode start offset
							_, err2 = metaEncoder.PutU64(uint64(curr))
							if err2 != nil {
								return 0, nil, err2
							}
							// end len
							_, err2 = metaEncoder.PutU64(uint64(len(storedFieldValues[i])))
							if err2 != nil {
								return 0, nil, err2
							}
							// encode number of array pos
							_, err2 = metaEncoder.PutU64(uint64(len(poss[uint16(fieldID)][i])))
							if err2 != nil {
								return 0, nil, err2
							}
							// encode all array positions
							for j := 0; j < len(poss[uint16(fieldID)][i]); j++ {
								_, err2 = metaEncoder.PutU64(poss[uint16(fieldID)][i][j])
								if err2 != nil {
									return 0, nil, err2
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
				compressed = snappy.Encode(compressed, data)
				// record where we're about to start writing
				docNumOffsets[newDocNum] = uint64(w.Count())

				// write out the meta len and compressed data len
				_, err = writeUvarints(w,
					uint64(len(metaBytes)), uint64(len(compressed)))
				if err != nil {
					return 0, nil, err
				}
				// now write the meta
				_, err = w.Write(metaBytes)
				if err != nil {
					return 0, nil, err
				}
				// now write the compressed data
				_, err = w.Write(compressed)
				if err != nil {
					return 0, nil, err
				}

				newDocNum++
			}
		}
		rv = append(rv, segNewDocNums)
	}

	// return value is the start of the stored index
	offset := uint64(w.Count())
	// now write out the stored doc index
	for docNum := range docNumOffsets {
		err := binary.Write(w, binary.BigEndian, docNumOffsets[docNum])
		if err != nil {
			return 0, nil, err
		}
	}

	return offset, rv, nil
}

// mergeFields builds a unified list of fields used across all the input segments
func mergeFields(segments []*Segment) []string {
	fieldsMap := map[string]struct{}{}

	for _, segment := range segments {
		fields := segment.Fields()
		for _, field := range fields {
			fieldsMap[field] = struct{}{}
		}
	}
	rv := make([]string, 0, len(fieldsMap))
	// ensure _id stays first
	rv = append(rv, "_id")
	for k := range fieldsMap {
		if k != "_id" {
			rv = append(rv, k)
		}
	}

	return rv
}
