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
	"io"
	"os"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/couchbaselabs/vellum"
	mmap "github.com/edsrzf/mmap-go"
	"github.com/golang/snappy"
)

// Open returns a zap impl of a segment
func Open(path string) (segment.Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		// mmap failed, try to close the file
		_ = f.Close()
		return nil, err
	}

	rv := &Segment{
		f:         f,
		mm:        mm,
		path:      path,
		fieldsMap: make(map[string]uint16),
	}

	err = rv.loadConfig()
	if err != nil {
		_ = rv.Close()
		return nil, err
	}

	err = rv.loadFields()
	if err != nil {
		_ = rv.Close()
		return nil, err
	}

	return rv, nil
}

// Segment implements the segment.Segment inteface over top the zap file format
type Segment struct {
	f                 *os.File
	mm                mmap.MMap
	path              string
	crc               uint32
	version           uint32
	chunkFactor       uint32
	numDocs           uint64
	storedIndexOffset uint64
	fieldsIndexOffset uint64

	fieldsMap     map[string]uint16
	fieldsInv     []string
	fieldsOffsets []uint64
}

func (s *Segment) loadConfig() error {
	crcOffset := len(s.mm) - 4
	s.crc = binary.BigEndian.Uint32(s.mm[crcOffset : crcOffset+4])
	verOffset := crcOffset - 4
	s.version = binary.BigEndian.Uint32(s.mm[verOffset : verOffset+4])
	if s.version != version {
		return fmt.Errorf("unsupported version %d", s.version)
	}
	chunkOffset := verOffset - 4
	s.chunkFactor = binary.BigEndian.Uint32(s.mm[chunkOffset : chunkOffset+4])
	fieldsOffset := chunkOffset - 8
	s.fieldsIndexOffset = binary.BigEndian.Uint64(s.mm[fieldsOffset : fieldsOffset+8])
	storedOffset := fieldsOffset - 8
	s.storedIndexOffset = binary.BigEndian.Uint64(s.mm[storedOffset : storedOffset+8])
	docNumOffset := storedOffset - 8
	s.numDocs = binary.BigEndian.Uint64(s.mm[docNumOffset : docNumOffset+8])
	return nil

}

func (s *Segment) loadFields() error {
	// NOTE for now we assume the fields index immediately preceeds the footer
	// if this changes, need to adjust accordingly (or store epxlicit length)
	fieldsIndexEnd := uint64(len(s.mm) - FooterSize)

	// iterate through fields index
	var fieldID uint64
	for s.fieldsIndexOffset+(8*fieldID) < fieldsIndexEnd {
		addr := binary.BigEndian.Uint64(s.mm[s.fieldsIndexOffset+(8*fieldID) : s.fieldsIndexOffset+(8*fieldID)+8])
		var n uint64

		dictLoc, read := binary.Uvarint(s.mm[addr+n : fieldsIndexEnd])
		n += uint64(read)
		s.fieldsOffsets = append(s.fieldsOffsets, dictLoc)

		var nameLen uint64
		nameLen, read = binary.Uvarint(s.mm[addr+n : fieldsIndexEnd])
		n += uint64(read)

		name := string(s.mm[addr+n : addr+n+nameLen])
		s.fieldsInv = append(s.fieldsInv, name)
		s.fieldsMap[name] = uint16(fieldID + 1)

		fieldID++
	}
	return nil
}

// Dictionary returns the term dictionary for the specified field
func (s *Segment) Dictionary(field string) (segment.TermDictionary, error) {
	dict, err := s.dictionary(field)
	if err == nil && dict == nil {
		return &segment.EmptyDictionary{}, nil
	}
	return dict, err
}

func (s *Segment) dictionary(field string) (*Dictionary, error) {
	rv := &Dictionary{
		segment: s,
		field:   field,
	}

	rv.fieldID = s.fieldsMap[field]
	if rv.fieldID > 0 {
		rv.fieldID = rv.fieldID - 1

		dictStart := s.fieldsOffsets[rv.fieldID]

		// read the length of the vellum data
		vellumLen, read := binary.Uvarint(s.mm[dictStart : dictStart+binary.MaxVarintLen64])
		fstBytes := s.mm[dictStart+uint64(read) : dictStart+uint64(read)+vellumLen]
		if fstBytes != nil {
			fst, err := vellum.Load(fstBytes)
			if err != nil {
				return nil, fmt.Errorf("dictionary field %s vellum err: %v", field, err)
			}
			if err == nil {
				rv.fst = fst
			}
		}

	} else {
		return nil, nil
	}

	return rv, nil
}

// VisitDocument invokes the DocFieldValueVistor for each stored field
// for the specified doc number
func (s *Segment) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	// first make sure this is a valid number in this segment
	if num < s.numDocs {
		docStoredStartAddr := s.storedIndexOffset + (8 * num)
		docStoredStart := binary.BigEndian.Uint64(s.mm[docStoredStartAddr : docStoredStartAddr+8])
		var n uint64
		metaLen, read := binary.Uvarint(s.mm[docStoredStart : docStoredStart+binary.MaxVarintLen64])
		n += uint64(read)
		var dataLen uint64
		dataLen, read = binary.Uvarint(s.mm[docStoredStart+n : docStoredStart+n+binary.MaxVarintLen64])
		n += uint64(read)
		meta := s.mm[docStoredStart+n : docStoredStart+n+metaLen]
		data := s.mm[docStoredStart+n+metaLen : docStoredStart+n+metaLen+dataLen]
		uncompressed, err := snappy.Decode(nil, data)
		if err != nil {
			panic(err)
		}
		// now decode meta and process
		reader := bytes.NewReader(meta)
		decoder := govarint.NewU64Base128Decoder(reader)

		keepGoing := true
		for keepGoing {
			field, err := decoder.GetU64()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			typ, err := decoder.GetU64()
			if err != nil {
				return err
			}
			offset, err := decoder.GetU64()
			if err != nil {
				return err
			}
			l, err := decoder.GetU64()
			if err != nil {
				return err
			}
			numap, err := decoder.GetU64()
			if err != nil {
				return err
			}
			var arrayPos []uint64
			if numap > 0 {
				arrayPos = make([]uint64, numap)
				for i := 0; i < int(numap); i++ {
					ap, err := decoder.GetU64()
					if err != nil {
						return err
					}
					arrayPos[i] = ap
				}
			}

			value := uncompressed[offset : offset+l]
			keepGoing = visitor(s.fieldsInv[field], byte(typ), value, arrayPos)
		}
	}
	return nil
}

// Count returns the number of documents in this segment.
func (s *Segment) Count() uint64 {
	return s.numDocs
}

// DocNumbers returns a bitset corresponding to the doc numbers of all the
// provided _id strings
func (s *Segment) DocNumbers(ids []string) (*roaring.Bitmap, error) {
	rv := roaring.New()

	if len(s.fieldsMap) > 0 {
		idDict, err := s.dictionary("_id")
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			postings, err := idDict.postingsList(id, nil)
			if err != nil {
				return nil, err
			}
			if postings.postings != nil {
				rv.Or(postings.postings)
			}
		}
	}

	return rv, nil
}

// Fields returns the field names used in this segment
func (s *Segment) Fields() []string {
	return s.fieldsInv
}

// Path returns the path of this segment on disk
func (s *Segment) Path() string {
	return s.path
}

// Close releases all resources associated with this segment
func (s *Segment) Close() (err error) {
	if s.mm != nil {
		err = s.mm.Unmap()
	}
	// try to close file even if unmap failed
	if s.f != nil {
		err2 := s.f.Close()
		if err == nil {
			// try to return first error
			err = err2
		}
	}
	return
}

// some helpers i started adding for the command-line utility

// Data returns the underlying mmaped data slice
func (s *Segment) Data() []byte {
	return s.mm
}

// CRC returns the CRC value stored in the file footer
func (s *Segment) CRC() uint32 {
	return s.crc
}

// Version returns the file version in the file footer
func (s *Segment) Version() uint32 {
	return s.version
}

// ChunkFactor returns the chunk factor in the file footer
func (s *Segment) ChunkFactor() uint32 {
	return s.chunkFactor
}

// FieldsIndexOffset returns the fields index offset in the file footer
func (s *Segment) FieldsIndexOffset() uint64 {
	return s.fieldsIndexOffset
}

// StoredIndexOffset returns the stored value index offset in the file foooter
func (s *Segment) StoredIndexOffset() uint64 {
	return s.storedIndexOffset
}

// NumDocs returns the number of documents in the file footer
func (s *Segment) NumDocs() uint64 {
	return s.numDocs
}

// DictAddr is a helper function to compute the file offset where the
// dictionary is stored for the specified field.
func (s *Segment) DictAddr(field string) (uint64, error) {
	var fieldID uint16
	var ok bool
	if fieldID, ok = s.fieldsMap[field]; !ok {
		return 0, fmt.Errorf("no such field '%s'", field)
	}

	return s.fieldsOffsets[fieldID-1], nil
}
