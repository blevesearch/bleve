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

	"github.com/RoaringBitmap/roaring"
)

// writes out the length of the roaring bitmap in bytes as varint
// then writs out the roaring bitmap itself
func writeRoaringWithLen(r *roaring.Bitmap, w io.Writer) (int, error) {
	var buffer bytes.Buffer
	// write out postings list to memory so we know the len
	postingsListLen, err := r.WriteTo(&buffer)
	if err != nil {
		return 0, err
	}
	var tw int
	// write out the length of this postings list
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(postingsListLen))
	nw, err := w.Write(buf[:n])
	tw += nw
	if err != nil {
		return tw, err
	}

	// write out the postings list itself
	nw, err = w.Write(buffer.Bytes())
	tw += nw
	if err != nil {
		return tw, err
	}

	return tw, nil
}

func persistFields(fieldsInv []string, w *CountHashWriter, dictLocs []uint64) (uint64, error) {
	var rv uint64

	var fieldStarts []uint64
	for fieldID, fieldName := range fieldsInv {

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
	for fieldID := range fieldsInv {
		err := binary.Write(w, binary.BigEndian, fieldStarts[fieldID])
		if err != nil {
			return 0, err
		}
	}

	return rv, nil
}

// FooterSize is the size of the footer record in bytes
// crc + ver + chunk + field offset + stored offset + num docs
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
