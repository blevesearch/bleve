//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/golang/protobuf/proto"
)

var TermFreqKeyPrefix = []byte{'t'}

type TermFreqRow struct {
	field  uint16
	term   []byte
	docID  []byte
	docNum uint64
	value  TermFreqValue
}

func NewTermVector(field uint16, pos uint64, start uint64, end uint64, arrayPos []uint64) *TermVector {
	rv := TermVector{}

	rv.Field = proto.Uint32(uint32(field))
	rv.Pos = proto.Uint64(pos)
	rv.Start = proto.Uint64(start)
	rv.End = proto.Uint64(end)

	if len(arrayPos) > 0 {
		rv.ArrayPositions = make([]uint64, len(arrayPos))
		for i, apv := range arrayPos {
			rv.ArrayPositions[i] = apv
		}
	}

	return &rv
}

func NewTermFreqRow(field uint16, term []byte, docID []byte, docNum uint64, freq uint64, norm float32, termVectors []*TermVector) *TermFreqRow {
	return InitTermFreqRow(&TermFreqRow{}, field, term, docID, docNum, freq, norm, termVectors)
}

func InitTermFreqRow(tfr *TermFreqRow, field uint16, term []byte, docID []byte, docNum uint64, freq uint64, norm float32, termVectors []*TermVector) *TermFreqRow {
	tfr.field = field
	tfr.term = term
	tfr.docID = docID
	tfr.docNum = docNum
	tfr.value.Freq = proto.Uint64(freq)
	tfr.value.Norm = proto.Float32(norm)
	tfr.value.Vectors = termVectors
	return tfr
}

func NewTermFreqRowKV(key, value []byte) (*TermFreqRow, error) {
	rv := TermFreqRow{}
	err := rv.ParseKey(key)
	if err != nil {
		return nil, err
	}
	err = rv.value.Unmarshal(value)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (tfr *TermFreqRow) ParseKey(key []byte) error {
	keyLen := len(key)
	if keyLen < 3 {
		return fmt.Errorf("invalid term frequency key, no valid field")
	}
	tfr.field = binary.LittleEndian.Uint16(key[1:3])

	termStartPos := 3
	termEndPos := bytes.IndexByte(key[termStartPos:], ByteSeparator)
	if termEndPos < 0 {
		return fmt.Errorf("invalid term frequency key, no byte separator terminating term")
	}
	tfr.term = key[termStartPos : termStartPos+termEndPos]

	docStartPos := termStartPos + termEndPos + 1
	docEndPos := bytes.IndexByte(key[docStartPos:], ByteSeparator)
	tfr.docID = key[docStartPos : docStartPos+docEndPos]

	docNumPos := docStartPos + docEndPos + 1
	tfr.docNum, _ = binary.Uvarint(key[docNumPos:])

	return nil
}

func (tfr *TermFreqRow) KeySize() int {
	return 3 + len(tfr.term) + 1 + len(tfr.docID) + 1 + binary.MaxVarintLen64
}

func (tfr *TermFreqRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
	docLen := copy(buf[3+termLen+1:], tfr.docID)
	buf[3+termLen+1+docLen] = ByteSeparator
	used := binary.PutUvarint(buf[3+termLen+1+docLen+1:], tfr.docNum)
	return 3 + termLen + 1 + docLen + 1 + used, nil
}

func (tfr *TermFreqRow) Key() []byte {
	buf := make([]byte, tfr.KeySize())
	n, _ := tfr.KeyTo(buf)
	return buf[:n]
}

func (tfr *TermFreqRow) ValueSize() int {
	return tfr.value.Size()
}

func (tfr *TermFreqRow) ValueTo(buf []byte) (int, error) {
	return tfr.value.MarshalTo(buf)
}

func (tfr *TermFreqRow) Value() []byte {
	buf := make([]byte, tfr.ValueSize())
	n, _ := tfr.ValueTo(buf)
	return buf[:n]
}

func (tfr *TermFreqRow) String() string {
	vectors := ""
	for i, v := range tfr.value.GetVectors() {
		vectors += fmt.Sprintf("%d - Field: %d Pos: %d Start: %d End: %d ArrayPos: %v - %#v\n", i, v.GetField(), v.GetPos(), v.GetStart(), v.GetEnd(), v.GetArrayPositions(), v.ArrayPositions)
	}
	return fmt.Sprintf("TermFreqRow - Field: %d\n", tfr.field) +
		fmt.Sprintf("Term '%s' - % x\n", tfr.term, tfr.term) +
		fmt.Sprintf("DocID '%s' - % x\n", tfr.docID, tfr.docID) +
		fmt.Sprintf("DocNum %d\n", tfr.docNum) +
		fmt.Sprintf("Freq: %d\n", tfr.value.GetFreq()) +
		fmt.Sprintf("Norm: %f\n", tfr.value.GetNorm()) +
		fmt.Sprintf("Vectors:\n%s", vectors)
}

func (tfr *TermFreqRow) Field() uint16 {
	return tfr.field
}

func (tfr *TermFreqRow) Term() []byte {
	return tfr.term
}

func (tfr *TermFreqRow) DocID() []byte {
	return tfr.docID
}

func (tfr *TermFreqRow) DocNum() uint64 {
	return tfr.docNum
}

func (tfr *TermFreqRow) Norm() float32 {
	return tfr.value.GetNorm()
}

func (tfr *TermFreqRow) Freq() uint64 {
	return tfr.value.GetFreq()
}

func (tfr *TermFreqRow) Vectors() []*TermVector {
	return tfr.value.GetVectors()
}

func (tfr *TermFreqRow) DictionaryRowKeySize() int {
	return 3 + len(tfr.term)
}

func (tfr *TermFreqRow) DictionaryRowKeyTo(buf []byte) (int, error) {
	dr := NewDictionaryRow(tfr.field, tfr.term, 0)
	return dr.KeyTo(buf)
}

func (tfr *TermFreqRow) DictionaryRowKey() []byte {
	dr := NewDictionaryRow(tfr.field, tfr.term, 0)
	return dr.Key()
}

func TermFreqIteratorStart(field uint16, term []byte) []byte {
	buf := make([]byte, 3+len(term)+1)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], field)
	termLen := copy(buf[3:], term)
	buf[3+termLen] = ByteSeparator
	return buf
}

func TermFreqPrefixFieldTermDocId(field uint16, term []byte, docID []byte) []byte {
	buf := make([]byte, 3+len(term)+1+len(docID)+1)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], field)
	termLen := copy(buf[3:], term)
	buf[3+termLen] = ByteSeparator
	docLen := copy(buf[3+termLen+1:], docID)
	buf[3+termLen+1+docLen] = ByteSeparator
	return buf
}
