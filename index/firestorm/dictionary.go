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
	"io"

	"github.com/golang/protobuf/proto"
)

const ByteSeparator byte = 0xff

var DictionaryKeyPrefix = []byte{'d'}

type DictionaryRow struct {
	field uint16
	term  []byte
	value DictionaryValue
}

func NewDictionaryRow(field uint16, term []byte, count uint64) *DictionaryRow {
	rv := DictionaryRow{
		field: field,
		term:  term,
	}
	rv.value.Count = proto.Uint64(count)
	return &rv
}

func NewDictionaryRowK(key []byte) (*DictionaryRow, error) {
	rv := DictionaryRow{}
	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.term, err = buf.ReadBytes(ByteSeparator)
	// there is no separator expected here, should get EOF
	if err != io.EOF {
		return nil, err
	}

	return &rv, nil
}

func (dr *DictionaryRow) parseDictionaryV(value []byte) error {
	err := dr.value.Unmarshal(value)
	if err != nil {
		return err
	}
	return nil
}

func NewDictionaryRowKV(key, value []byte) (*DictionaryRow, error) {
	rv, err := NewDictionaryRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseDictionaryV(value)
	if err != nil {
		return nil, err
	}
	return rv, nil

}

func (dr *DictionaryRow) Count() uint64 {
	return dr.value.GetCount()
}

func (dr *DictionaryRow) SetCount(count uint64) {
	dr.value.Count = proto.Uint64(count)
}

func (dr *DictionaryRow) KeySize() int {
	return 3 + len(dr.term)
}

func (dr *DictionaryRow) KeyTo(buf []byte) (int, error) {
	copy(buf[0:], DictionaryKeyPrefix)
	binary.LittleEndian.PutUint16(buf[1:3], dr.field)
	copy(buf[3:], dr.term)
	return 3 + len(dr.term), nil
}

func (dr *DictionaryRow) Key() []byte {
	buf := make([]byte, dr.KeySize())
	n, _ := dr.KeyTo(buf)
	return buf[:n]
}

func (dr *DictionaryRow) ValueSize() int {
	return dr.value.Size()
}

func (dr *DictionaryRow) ValueTo(buf []byte) (int, error) {
	return dr.value.MarshalTo(buf)
}

func (dr *DictionaryRow) Value() []byte {
	buf := make([]byte, dr.ValueSize())
	n, _ := dr.ValueTo(buf)
	return buf[:n]
}

func DictionaryRowKey(field uint16, term []byte) []byte {
	buf := make([]byte, 3+len(term))
	copy(buf[0:], DictionaryKeyPrefix)
	binary.LittleEndian.PutUint16(buf[1:3], field)
	copy(buf[3:], term)
	return buf
}
