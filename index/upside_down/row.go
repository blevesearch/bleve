//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package upside_down

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const BYTE_SEPARATOR byte = 0xff

type UpsideDownCouchRowStream chan UpsideDownCouchRow

type UpsideDownCouchRow interface {
	Key() []byte
	Value() []byte
}

func ParseFromKeyValue(key, value []byte) (UpsideDownCouchRow, error) {
	if len(key) > 0 {
		switch key[0] {
		case 'v':
			return NewVersionRowKV(key, value)
		case 'f':
			return NewFieldRowKV(key, value)
		case 't':
			return NewTermFrequencyRowKV(key, value)
		case 'b':
			return NewBackIndexRowKV(key, value)
		case 's':
			return NewStoredRowKV(key, value)
		}
		return nil, fmt.Errorf("Unknown field type '%s'", string(key[0]))
	}
	return nil, fmt.Errorf("Invalid empty key")
}

// VERSION

type VersionRow struct {
	version uint8
}

func (v *VersionRow) Key() []byte {
	return []byte{'v'}
}

func (v *VersionRow) Value() []byte {
	return []byte{byte(v.version)}
}

func (v *VersionRow) String() string {
	return fmt.Sprintf("Version: %d", v.version)
}

func NewVersionRow(version uint8) *VersionRow {
	return &VersionRow{
		version: version,
	}
}

func NewVersionRowKV(key, value []byte) (*VersionRow, error) {
	rv := VersionRow{}
	buf := bytes.NewBuffer(value)
	err := binary.Read(buf, binary.LittleEndian, &rv.version)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

// FIELD definition

type FieldRow struct {
	index uint16
	name  string
}

func (f *FieldRow) Key() []byte {
	buf := make([]byte, 3)
	buf[0] = 'f'
	binary.LittleEndian.PutUint16(buf[1:3], f.index)
	return buf
}

func (f *FieldRow) Value() []byte {
	return append([]byte(f.name), BYTE_SEPARATOR)
}

func (f *FieldRow) String() string {
	return fmt.Sprintf("Field: %d Name: %s", f.index, f.name)
}

func NewFieldRow(index uint16, name string) *FieldRow {
	return &FieldRow{
		index: index,
		name:  name,
	}
}

func NewFieldRowKV(key, value []byte) (*FieldRow, error) {
	rv := FieldRow{}

	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type
	err := binary.Read(buf, binary.LittleEndian, &rv.index)
	if err != nil {
		return nil, err
	}

	buf = bytes.NewBuffer(value)
	rv.name, err = buf.ReadString(BYTE_SEPARATOR)
	if err != nil {
		return nil, err
	}
	rv.name = rv.name[:len(rv.name)-1] // trim off separator byte

	return &rv, nil
}

// TERM FIELD FREQUENCY

type TermVector struct {
	field uint16
	pos   uint64
	start uint64
	end   uint64
}

func (tv *TermVector) String() string {
	return fmt.Sprintf("Field: %d Pos: %d Start: %d End %d", tv.field, tv.pos, tv.start, tv.end)
}

type TermFrequencyRow struct {
	term    []byte
	field   uint16
	doc     []byte
	freq    uint64
	norm    float32
	vectors []*TermVector
}

func (tfr *TermFrequencyRow) Key() []byte {
	buf := make([]byte, 3+len(tfr.term)+1+len(tfr.doc))
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = BYTE_SEPARATOR
	copy(buf[3+termLen+1:], tfr.doc)
	return buf
}

func (tfr *TermFrequencyRow) Value() []byte {
	buf := make([]byte, 8+4+(len(tfr.vectors)*(2+8+8+8)))

	binary.LittleEndian.PutUint64(buf[0:8], tfr.freq)

	normuint32 := math.Float32bits(tfr.norm)
	binary.LittleEndian.PutUint32(buf[8:12], normuint32)

	offset := 12
	for _, vector := range tfr.vectors {
		binary.LittleEndian.PutUint16(buf[offset:offset+2], vector.field)
		binary.LittleEndian.PutUint64(buf[offset+2:offset+10], vector.pos)
		binary.LittleEndian.PutUint64(buf[offset+10:offset+18], vector.start)
		binary.LittleEndian.PutUint64(buf[offset+18:offset+26], vector.end)
		offset += 26
	}
	return buf
}

func (tfr *TermFrequencyRow) String() string {
	return fmt.Sprintf("Term: `%s` Field: %d DocId: `%s` Frequency: %d Norm: %f Vectors: %v", string(tfr.term), tfr.field, string(tfr.doc), tfr.freq, tfr.norm, tfr.vectors)
}

func NewTermFrequencyRow(term []byte, field uint16, doc string, freq uint64, norm float32) *TermFrequencyRow {
	return &TermFrequencyRow{
		term:  term,
		field: field,
		doc:   []byte(doc),
		freq:  freq,
		norm:  norm,
	}
}

func NewTermFrequencyRowWithTermVectors(term []byte, field uint16, doc string, freq uint64, norm float32, vectors []*TermVector) *TermFrequencyRow {
	return &TermFrequencyRow{
		term:    term,
		field:   field,
		doc:     []byte(doc),
		freq:    freq,
		norm:    norm,
		vectors: vectors,
	}
}

func NewTermFrequencyRowKV(key, value []byte) (*TermFrequencyRow, error) {
	rv := TermFrequencyRow{
		doc: []byte(""),
	}
	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type

	var err error
	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.term, err = buf.ReadBytes(BYTE_SEPARATOR)
	if err != nil {
		return nil, err
	}
	rv.term = rv.term[:len(rv.term)-1] // trim off separator byte

	doc, err := buf.ReadBytes(BYTE_SEPARATOR)
	if err != io.EOF {
		return nil, err
	}
	if doc != nil {
		rv.doc = doc
	}

	buf = bytes.NewBuffer((value))
	err = binary.Read(buf, binary.LittleEndian, &rv.freq)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &rv.norm)
	if err != nil {
		return nil, err
	}

	var field uint16
	err = binary.Read(buf, binary.LittleEndian, &field)
	if err != nil && err != io.EOF {
		return nil, err
	}
	for err != io.EOF {
		tv := TermVector{}
		tv.field = field
		// at this point we expect at least one term vector
		if rv.vectors == nil {
			rv.vectors = make([]*TermVector, 0)
		}

		err = binary.Read(buf, binary.LittleEndian, &tv.pos)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buf, binary.LittleEndian, &tv.start)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buf, binary.LittleEndian, &tv.end)
		if err != nil {
			return nil, err
		}
		rv.vectors = append(rv.vectors, &tv)
		// try to read next record (may not exist)
		err = binary.Read(buf, binary.LittleEndian, &field)
	}

	return &rv, nil

}

type BackIndexEntry struct {
	term  []byte
	field uint16
}

func (bie *BackIndexEntry) String() string {
	return fmt.Sprintf("Term: `%s` Field: %d", string(bie.term), bie.field)
}

type BackIndexRow struct {
	doc          []byte
	entries      []*BackIndexEntry
	storedFields []uint16
}

func (br *BackIndexRow) Key() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('b')
	buf.Write(br.doc)
	return buf.Bytes()
}

func (br *BackIndexRow) Value() []byte {
	buf := new(bytes.Buffer)
	for _, e := range br.entries {
		buf.Write(e.term)
		buf.WriteByte(BYTE_SEPARATOR)
		fieldbuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(fieldbuf, e.field)
		buf.Write(fieldbuf)
	}
	for _, sf := range br.storedFields {
		buf.WriteByte(BYTE_SEPARATOR)
		fieldbuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(fieldbuf, sf)
		buf.Write(fieldbuf)
	}
	return buf.Bytes()
}

func (br *BackIndexRow) String() string {
	return fmt.Sprintf("Backindex DocId: `%s` Entries: %v, Stored Fields: %v", string(br.doc), br.entries, br.storedFields)
}

func NewBackIndexRow(doc string, entries []*BackIndexEntry, storedFields []uint16) *BackIndexRow {
	return &BackIndexRow{
		doc:          []byte(doc),
		entries:      entries,
		storedFields: storedFields,
	}
}

func NewBackIndexRowKV(key, value []byte) (*BackIndexRow, error) {
	rv := BackIndexRow{}

	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type

	var err error
	rv.doc, err = buf.ReadBytes(BYTE_SEPARATOR)
	if err == io.EOF && len(rv.doc) < 1 {
		err = fmt.Errorf("invalid doc length 0")
	}
	if err != io.EOF {
		return nil, err
	}

	buf = bytes.NewBuffer(value)
	rv.entries = make([]*BackIndexEntry, 0)
	rv.storedFields = make([]uint16, 0)

	var term []byte
	term, err = buf.ReadBytes(BYTE_SEPARATOR)
	if err == io.EOF && len(term) < 1 {
		err = fmt.Errorf("invalid term length 0")
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	for err != io.EOF {
		if len(term) > 2 {
			// this is a back index entry
			ent := BackIndexEntry{}
			ent.term = term[:len(term)-1] // trim off separator byte

			err = binary.Read(buf, binary.LittleEndian, &ent.field)
			if err != nil {
				return nil, err
			}
			rv.entries = append(rv.entries, &ent)
		} else {
			// this is a stored field entry
			var sf uint16
			err = binary.Read(buf, binary.LittleEndian, &sf)
			if err != nil {
				return nil, err
			}
			rv.storedFields = append(rv.storedFields, sf)
		}

		term, err = buf.ReadBytes(BYTE_SEPARATOR)
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	return &rv, nil
}

// STORED

type StoredRow struct {
	doc   []byte
	field uint16
	typ   byte
	value []byte
}

func (s *StoredRow) Key() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('s')
	buf.Write(s.doc)
	buf.WriteByte(BYTE_SEPARATOR)
	fieldbuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(fieldbuf, s.field)
	buf.Write(fieldbuf)
	return buf.Bytes()
}

func (s *StoredRow) Value() []byte {
	rv := make([]byte, len(s.value)+1)
	rv[0] = s.typ
	copy(rv[1:], s.value)
	return rv
}

func (s *StoredRow) String() string {
	return fmt.Sprintf("Document: %s Field %d, Type: %s Value: %s", s.doc, s.field, string(s.typ), s.value)
}

func (s *StoredRow) ScanPrefixForDoc() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('s')
	buf.Write(s.doc)
	buf.WriteByte(BYTE_SEPARATOR)
	return buf.Bytes()
}

func NewStoredRow(doc string, field uint16, typ byte, value []byte) *StoredRow {
	return &StoredRow{
		doc:   []byte(doc),
		field: field,
		typ:   typ,
		value: value,
	}
}

func NewStoredRowKV(key, value []byte) (*StoredRow, error) {
	rv := StoredRow{}

	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type

	var err error
	rv.doc, err = buf.ReadBytes(BYTE_SEPARATOR)
	if len(rv.doc) < 2 { // 1 for min doc id length, 1 for separator
		err = fmt.Errorf("invalid doc length 0")
		return nil, err
	}

	rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.typ = value[0]

	rv.value = value[1:]

	return &rv, nil
}
