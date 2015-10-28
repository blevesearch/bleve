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

	"github.com/blevesearch/bleve/index/store"
	"github.com/golang/protobuf/proto"
)

var FieldKeyPrefix = []byte{'f'}

func (f *Firestorm) fieldIndexOrNewRow(name string) (uint16, *FieldRow) {
	index, existed := f.fieldCache.FieldNamed(name, true)
	if !existed {
		return index, NewFieldRow(uint16(index), name)
	}
	return index, nil
}

func (f *Firestorm) loadFields(reader store.KVReader) (err error) {

	err = visitPrefix(reader, FieldKeyPrefix, func(key, val []byte) (bool, error) {
		fieldRow, err := NewFieldRowKV(key, val)
		if err != nil {
			return false, err
		}
		f.fieldCache.AddExisting(fieldRow.Name(), fieldRow.Index())
		return true, nil
	})

	return
}

type FieldRow struct {
	index uint16
	value FieldValue
}

func NewFieldRow(i uint16, name string) *FieldRow {
	rv := FieldRow{
		index: i,
	}
	rv.value.Name = proto.String(name)
	return &rv
}

func NewFieldRowKV(key, value []byte) (*FieldRow, error) {
	rv := FieldRow{}

	buf := bytes.NewBuffer(key)
	_, err := buf.ReadByte() // type
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &rv.index)
	if err != nil {
		return nil, err
	}

	err = rv.value.Unmarshal(value)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (fr *FieldRow) KeySize() int {
	return 3
}

func (fr *FieldRow) KeyTo(buf []byte) (int, error) {
	buf[0] = 'f'
	binary.LittleEndian.PutUint16(buf[1:3], fr.index)
	return 3, nil
}

func (fr *FieldRow) Key() []byte {
	buf := make([]byte, fr.KeySize())
	n, _ := fr.KeyTo(buf)
	return buf[:n]
}

func (fr *FieldRow) ValueSize() int {
	return fr.value.Size()
}

func (fr *FieldRow) ValueTo(buf []byte) (int, error) {
	return fr.value.MarshalTo(buf)
}

func (fr *FieldRow) Value() []byte {
	buf := make([]byte, fr.ValueSize())
	n, _ := fr.ValueTo(buf)
	return buf[:n]
}

func (fr *FieldRow) Index() uint16 {
	return fr.index
}

func (fr *FieldRow) Name() string {
	return fr.value.GetName()
}

func (fr *FieldRow) String() string {
	return fmt.Sprintf("FieldRow - Field: %d - Name: %s\n", fr.index, fr.Name())
}
