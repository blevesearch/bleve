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
)

var StoredKeyPrefix = []byte{'s'}

type StoredRow struct {
	docID          []byte
	docNum         uint64
	field          uint16
	arrayPositions []uint64
	value          StoredValue
}

func NewStoredRow(docID []byte, docNum uint64, field uint16, arrayPositions []uint64, value []byte) *StoredRow {
	rv := StoredRow{
		docID:          docID,
		docNum:         docNum,
		field:          field,
		arrayPositions: arrayPositions,
	}
	if len(arrayPositions) < 1 {
		rv.arrayPositions = make([]uint64, 0)
	}
	rv.value.Raw = value // FIXME review do we need to copy?
	return &rv
}

func NewStoredRowKV(key, value []byte) (*StoredRow, error) {
	rv := StoredRow{}
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

func (sr *StoredRow) ParseKey(key []byte) error {
	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return err
	}

	sr.docID, err = buf.ReadBytes(ByteSeparator)
	if len(sr.docID) < 2 { // 1 for min doc id length, 1 for separator
		err = fmt.Errorf("invalid doc length 0")
		return err
	}

	sr.docID = sr.docID[:len(sr.docID)-1] // trim off separator byte

	sr.docNum, err = binary.ReadUvarint(buf)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &sr.field)
	if err != nil {
		return err
	}

	sr.arrayPositions = make([]uint64, 0)
	nextArrayPos, err := binary.ReadUvarint(buf)
	for err == nil {
		sr.arrayPositions = append(sr.arrayPositions, nextArrayPos)
		nextArrayPos, err = binary.ReadUvarint(buf)
	}

	return nil
}

func (sr *StoredRow) KeySize() int {
	return 1 + len(sr.docID) + 1 + binary.MaxVarintLen64 + 2 + (binary.MaxVarintLen64 * len(sr.arrayPositions))
}

func (sr *StoredRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 's'
	copy(buf[1:], sr.docID)
	buf[1+len(sr.docID)] = ByteSeparator
	bytesUsed := 1 + len(sr.docID) + 1
	bytesUsed += binary.PutUvarint(buf[bytesUsed:], sr.docNum)
	binary.LittleEndian.PutUint16(buf[bytesUsed:], sr.field)
	bytesUsed += 2
	for _, arrayPosition := range sr.arrayPositions {
		varbytes := binary.PutUvarint(buf[bytesUsed:], arrayPosition)
		bytesUsed += varbytes
	}
	return bytesUsed, nil
}

func (sr *StoredRow) Key() []byte {

	buf := make([]byte, sr.KeySize())
	n, _ := sr.KeyTo(buf)
	return buf[:n]
}

func (sr *StoredRow) ValueSize() int {
	return sr.value.Size()
}

func (sr *StoredRow) ValueTo(buf []byte) (int, error) {
	return sr.value.MarshalTo(buf)
}

func (sr *StoredRow) Value() []byte {
	buf := make([]byte, sr.ValueSize())
	n, _ := sr.ValueTo(buf)
	return buf[:n]
}

func (sr *StoredRow) DocID() []byte {
	return sr.docID
}

func (sr *StoredRow) DocNum() uint64 {
	return sr.docNum
}

func (sr *StoredRow) String() string {
	return fmt.Sprintf("StoredRow - Field: %d\n", sr.field) +
		fmt.Sprintf("DocID '%s' - % x\n", sr.docID, sr.docID) +
		fmt.Sprintf("DocNum %d\n", sr.docNum) +
		fmt.Sprintf("Array Positions:\n%v", sr.arrayPositions) +
		fmt.Sprintf("Value: % x", sr.value.GetRaw())
}

func StoredIteratorStartDocID(docID []byte) []byte {
	docLen := len(docID)
	buf := make([]byte, 1+docLen+1)
	buf[0] = 's'
	copy(buf[1:], docID)
	buf[1+docLen] = ByteSeparator
	return buf
}

func StoredPrefixDocIDNum(docID []byte, docNum uint64) []byte {
	docLen := len(docID)
	buf := make([]byte, 1+docLen+1+binary.MaxVarintLen64)
	buf[0] = 's'
	copy(buf[1:], docID)
	buf[1+docLen] = ByteSeparator
	bytesUsed := 1 + docLen + 1
	bytesUsed += binary.PutUvarint(buf[bytesUsed:], docNum)
	return buf[0:bytesUsed]
}
