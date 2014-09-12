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

	"code.google.com/p/goprotobuf/proto"
)

const ByteSeparator byte = 0xff

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
		case 'i':
			return NewInternalRowKV(key, value)
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

// INTERNAL STORAGE

type InternalRow struct {
	key []byte
	val []byte
}

func (i *InternalRow) Key() []byte {
	buf := make([]byte, len(i.key)+1)
	buf[0] = 'i'
	copy(buf[1:], i.key)
	return buf
}

func (i *InternalRow) Value() []byte {
	return i.val
}

func (i *InternalRow) String() string {
	return fmt.Sprintf("InternalStore - Key: %s (% x) Val: %s (% x)", i.key, i.key, i.val, i.val)
}

func NewInternalRow(key, val []byte) *InternalRow {
	return &InternalRow{
		key: key,
		val: val,
	}
}

func NewInternalRowKV(key, value []byte) (*InternalRow, error) {
	rv := InternalRow{}
	rv.key = key[1:]
	rv.val = value
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
	return append([]byte(f.name), ByteSeparator)
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
	rv.name, err = buf.ReadString(ByteSeparator)
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

func (tfr *TermFrequencyRow) ScanPrefixForField() []byte {
	buf := make([]byte, 3)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTermPrefix() []byte {
	buf := make([]byte, 3+len(tfr.term))
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	copy(buf[3:], tfr.term)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTerm() []byte {
	buf := make([]byte, 3+len(tfr.term)+1)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
	return buf
}

func (tfr *TermFrequencyRow) Key() []byte {
	buf := make([]byte, 3+len(tfr.term)+1+len(tfr.doc))
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
	copy(buf[3+termLen+1:], tfr.doc)
	return buf
}

func (tfr *TermFrequencyRow) SummaryKey() []byte {
	buf := make([]byte, 3+len(tfr.term)+1)
	buf[0] = 't'
	binary.LittleEndian.PutUint16(buf[1:3], tfr.field)
	termLen := copy(buf[3:], tfr.term)
	buf[3+termLen] = ByteSeparator
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

func NewTermFrequencyRowK(key []byte) (*TermFrequencyRow, error) {
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

	rv.term, err = buf.ReadBytes(ByteSeparator)
	if err != nil {
		return nil, err
	}
	rv.term = rv.term[:len(rv.term)-1] // trim off separator byte

	doc, err := buf.ReadBytes(ByteSeparator)
	if err != io.EOF {
		return nil, err
	}
	if doc != nil {
		rv.doc = doc
	}

	return &rv, nil
}

func (tfr *TermFrequencyRow) parseV(value []byte) error {
	buf := bytes.NewBuffer((value))
	err := binary.Read(buf, binary.LittleEndian, &tfr.freq)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &tfr.norm)
	if err != nil {
		return err
	}

	var field uint16
	err = binary.Read(buf, binary.LittleEndian, &field)
	if err != nil && err != io.EOF {
		return err
	}
	for err != io.EOF {
		tv := TermVector{}
		tv.field = field
		// at this point we expect at least one term vector
		if tfr.vectors == nil {
			tfr.vectors = make([]*TermVector, 0)
		}

		err = binary.Read(buf, binary.LittleEndian, &tv.pos)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &tv.start)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &tv.end)
		if err != nil {
			return err
		}
		tfr.vectors = append(tfr.vectors, &tv)
		// try to read next record (may not exist)
		err = binary.Read(buf, binary.LittleEndian, &field)
	}
	return nil
}

func NewTermFrequencyRowKV(key, value []byte) (*TermFrequencyRow, error) {
	rv, err := NewTermFrequencyRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseV(value)
	if err != nil {
		return nil, err
	}
	return rv, nil

}

type BackIndexRow struct {
	doc           []byte
	termEntries   []*BackIndexTermEntry
	storedEntries []*BackIndexStoreEntry
}

func (br *BackIndexRow) AllTermKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, len(br.termEntries))
	for i, termEntry := range br.termEntries {
		termRow := NewTermFrequencyRow([]byte(termEntry.GetTerm()), uint16(termEntry.GetField()), string(br.doc), 0, 0)
		rv[i] = termRow.Key()
	}
	return rv
}

func (br *BackIndexRow) AllStoredKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, len(br.storedEntries))
	for i, storedEntry := range br.storedEntries {
		storedRow := NewStoredRow(string(br.doc), uint16(storedEntry.GetField()), storedEntry.GetArrayPositions(), 'x', []byte{})
		rv[i] = storedRow.Key()
	}
	return rv
}

func (br *BackIndexRow) Key() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('b')
	buf.Write(br.doc)
	return buf.Bytes()
}

func (br *BackIndexRow) Value() []byte {
	birv := &BackIndexRowValue{
		TermEntries:   br.termEntries,
		StoredEntries: br.storedEntries,
	}
	bytes, _ := proto.Marshal(birv)
	return bytes
}

func (br *BackIndexRow) String() string {
	return fmt.Sprintf("Backindex DocId: `%s` Term Entries: %v, Stored Entries: %v", string(br.doc), br.termEntries, br.storedEntries)
}

func NewBackIndexRow(doc string, entries []*BackIndexTermEntry, storedFields []*BackIndexStoreEntry) *BackIndexRow {
	return &BackIndexRow{
		doc:           []byte(doc),
		termEntries:   entries,
		storedEntries: storedFields,
	}
}

func NewBackIndexRowKV(key, value []byte) (*BackIndexRow, error) {
	rv := BackIndexRow{}

	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type

	var err error
	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if err == io.EOF && len(rv.doc) < 1 {
		err = fmt.Errorf("invalid doc length 0")
	}
	if err != io.EOF {
		return nil, err
	}

	var birv BackIndexRowValue
	err = proto.Unmarshal(value, &birv)
	if err != nil {
		return nil, err
	}
	rv.termEntries = birv.TermEntries
	rv.storedEntries = birv.StoredEntries

	return &rv, nil
}

// STORED

type StoredRow struct {
	doc            []byte
	field          uint16
	arrayPositions []uint64
	typ            byte
	value          []byte
}

func (s *StoredRow) Key() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('s')
	buf.Write(s.doc)
	buf.WriteByte(ByteSeparator)
	fieldbuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(fieldbuf, s.field)
	buf.Write(fieldbuf)
	for _, arrayPosition := range s.arrayPositions {
		arrayPositionBuffer := make([]byte, binary.MaxVarintLen64)
		numBytes := binary.PutUvarint(arrayPositionBuffer, arrayPosition)
		buf.Write(arrayPositionBuffer[0:numBytes])
	}
	return buf.Bytes()
}

func (s *StoredRow) Value() []byte {
	rv := make([]byte, len(s.value)+1)
	rv[0] = s.typ
	copy(rv[1:], s.value)
	return rv
}

func (s *StoredRow) String() string {
	return fmt.Sprintf("Document: %s Field %d, Array Positions: %v, Type: %s Value: %s", s.doc, s.field, s.arrayPositions, string(s.typ), s.value)
}

func (s *StoredRow) ScanPrefixForDoc() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte('s')
	buf.Write(s.doc)
	buf.WriteByte(ByteSeparator)
	return buf.Bytes()
}

func NewStoredRow(doc string, field uint16, arrayPositions []uint64, typ byte, value []byte) *StoredRow {
	return &StoredRow{
		doc:            []byte(doc),
		field:          field,
		arrayPositions: arrayPositions,
		typ:            typ,
		value:          value,
	}
}

func NewStoredRowK(key []byte) (*StoredRow, error) {
	rv := StoredRow{}

	buf := bytes.NewBuffer(key)
	buf.ReadByte() // type

	var err error
	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if len(rv.doc) < 2 { // 1 for min doc id length, 1 for separator
		err = fmt.Errorf("invalid doc length 0")
		return nil, err
	}

	rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.arrayPositions = make([]uint64, 0)
	nextArrayPos, err := binary.ReadUvarint(buf)
	for err == nil {
		rv.arrayPositions = append(rv.arrayPositions, nextArrayPos)
		nextArrayPos, err = binary.ReadUvarint(buf)
	}
	return &rv, nil
}

func NewStoredRowKV(key, value []byte) (*StoredRow, error) {
	rv, err := NewStoredRowK(key)
	if err != nil {
		return nil, err
	}
	rv.typ = value[0]
	rv.value = value[1:]
	return rv, nil
}
