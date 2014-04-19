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
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(v.version))
	return buf.Bytes()
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
	buf := new(bytes.Buffer)
	buf.WriteByte('f')
	indexbuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(indexbuf, f.index)
	buf.Write(indexbuf)
	return buf.Bytes()
}

func (f *FieldRow) Value() []byte {
	buf := new(bytes.Buffer)
	buf.WriteString(f.name)
	buf.WriteByte(BYTE_SEPARATOR)
	return buf.Bytes()
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
	buf := new(bytes.Buffer)
	buf.WriteByte('t')
	buf.Write(tfr.term)
	buf.WriteByte(BYTE_SEPARATOR)
	fieldbuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(fieldbuf, tfr.field)
	buf.Write(fieldbuf)
	buf.Write(tfr.doc)
	return buf.Bytes()
}

func (tfr *TermFrequencyRow) Value() []byte {
	buf := new(bytes.Buffer)

	freqbuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(freqbuf, tfr.freq)
	buf.Write(freqbuf)

	normuint32 := math.Float32bits(tfr.norm)
	normbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(normbuf, normuint32)
	buf.Write(normbuf)

	for _, vector := range tfr.vectors {
		fieldbuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(fieldbuf, vector.field)
		buf.Write(fieldbuf)
		posbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(posbuf, vector.pos)
		buf.Write(posbuf)
		startbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(startbuf, vector.start)
		buf.Write(startbuf)
		endbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(endbuf, vector.end)
		buf.Write(endbuf)
	}
	return buf.Bytes()
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
	rv.term, err = buf.ReadBytes(BYTE_SEPARATOR)
	if err != nil {
		return nil, err
	}
	rv.term = rv.term[:len(rv.term)-1] // trim off separator byte

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

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
	doc     []byte
	entries []*BackIndexEntry
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
	return buf.Bytes()
}

func (br *BackIndexRow) String() string {
	return fmt.Sprintf("Backindex DocId: `%s` Entries: %v", string(br.doc), br.entries)
}

func NewBackIndexRow(doc string, entries []*BackIndexEntry) *BackIndexRow {
	return &BackIndexRow{
		doc:     []byte(doc),
		entries: entries,
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

	var term []byte
	term, err = buf.ReadBytes(BYTE_SEPARATOR)
	if err == io.EOF && len(term) < 1 {
		err = fmt.Errorf("invalid term length 0")
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	for err != io.EOF {
		ent := BackIndexEntry{}
		ent.term = term[:len(term)-1] // trim off separator byte

		err = binary.Read(buf, binary.LittleEndian, &ent.field)
		if err != nil {
			return nil, err
		}
		rv.entries = append(rv.entries, &ent)

		term, err = buf.ReadBytes(BYTE_SEPARATOR)
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	return &rv, nil
}
