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
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type firestormReader struct {
	f        *Firestorm
	r        store.KVReader
	s        *Snapshot
	docCount uint64
}

func newFirestormReader(f *Firestorm) (index.IndexReader, error) {
	r, err := f.store.Reader()
	if err != nil {
		return nil, fmt.Errorf("error opening store reader: %v", err)
	}
	docCount, err := f.DocCount()
	if err != nil {
		return nil, fmt.Errorf("error opening store reader: %v", err)
	}
	rv := firestormReader{
		f:        f,
		r:        r,
		s:        f.compensator.Snapshot(),
		docCount: docCount,
	}
	return &rv, nil
}

func (r *firestormReader) TermFieldReader(term []byte, field string) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := r.f.fieldCache.FieldNamed(field, false)
	if fieldExists {
		return newFirestormTermFieldReader(r, uint16(fieldIndex), term)
	}
	return newFirestormTermFieldReader(r, ^uint16(0), []byte{ByteSeparator})
}

func (r *firestormReader) DocIDReader(start, end string) (index.DocIDReader, error) {
	return newFirestormDocIDReader(r, start, end)
}

func (r *firestormReader) FieldDict(field string) (index.FieldDict, error) {
	return r.FieldDictRange(field, nil, nil)
}

func (r *firestormReader) FieldDictRange(field string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	fieldIndex, fieldExists := r.f.fieldCache.FieldNamed(field, false)
	if fieldExists {
		return newFirestormDictionaryReader(r, uint16(fieldIndex), startTerm, endTerm)
	}
	return newFirestormDictionaryReader(r, ^uint16(0), []byte{ByteSeparator}, []byte{})
}

func (r *firestormReader) FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error) {
	return r.FieldDictRange(field, termPrefix, incrementBytes(termPrefix))
}

func (r *firestormReader) Document(id string) (*document.Document, error) {
	docID := []byte(id)
	docNum, err := r.currDocNumForId(docID)
	if err != nil {
		return nil, err
	} else if docNum == 0 {
		return nil, nil
	}
	rv := document.NewDocument(id)
	prefix := StoredPrefixDocIDNum(docID, docNum)
	err = visitPrefix(r.r, prefix, func(key, val []byte) (bool, error) {
		safeVal := make([]byte, len(val))
		copy(safeVal, val)
		row, err := NewStoredRowKV(key, safeVal)
		if err != nil {
			return false, err
		}
		if row != nil {
			fieldName := r.f.fieldCache.FieldIndexed(row.field)
			field := r.decodeFieldType(fieldName, row.arrayPositions, row.value.GetRaw())
			if field != nil {
				rv.AddField(field)
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (r *firestormReader) decodeFieldType(name string, pos []uint64, value []byte) document.Field {
	switch value[0] {
	case 't':
		return document.NewTextField(name, pos, value[1:])
	case 'n':
		return document.NewNumericFieldFromBytes(name, pos, value[1:])
	case 'd':
		return document.NewDateTimeFieldFromBytes(name, pos, value[1:])
	case 'b':
		return document.NewBooleanFieldFromBytes(name, pos, value[1:])
	}
	return nil
}

func (r *firestormReader) currDocNumForId(docID []byte) (uint64, error) {
	prefix := TermFreqPrefixFieldTermDocId(0, nil, docID)
	docNums := make(DocNumberList, 0)
	err := visitPrefix(r.r, prefix, func(key, val []byte) (bool, error) {
		tfk, err := NewTermFreqRowKV(key, val)
		if err != nil {
			return false, err
		}
		docNum := tfk.DocNum()
		docNums = append(docNums, docNum)
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	if len(docNums) > 0 {
		sort.Sort(docNums)
		return docNums[0], nil
	}
	return 0, nil
}

func (r *firestormReader) DocumentFieldTerms(id string) (index.FieldTerms, error) {

	docID := []byte(id)
	docNum, err := r.currDocNumForId(docID)
	if err != nil {
		return nil, err
	} else if docNum == 0 {
		return nil, nil
	}

	rv := make(index.FieldTerms, 0)
	// walk the term freqs
	err = visitPrefix(r.r, TermFreqKeyPrefix, func(key, val []byte) (bool, error) {
		tfr, err := NewTermFreqRowKV(key, val)
		if err != nil {
			return false, err
		}
		if bytes.Compare(tfr.DocID(), docID) == 0 && tfr.DocNum() == docNum && tfr.Field() != 0 {
			fieldName := r.f.fieldCache.FieldIndexed(uint16(tfr.Field()))
			terms, ok := rv[fieldName]
			if !ok {
				terms = make([]string, 0, 1)
			}
			terms = append(terms, string(tfr.Term()))
			rv[fieldName] = terms
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (r *firestormReader) Fields() ([]string, error) {
	fields := make([]string, 0)

	err := visitPrefix(r.r, FieldKeyPrefix, func(key, val []byte) (bool, error) {
		fieldRow, err := NewFieldRowKV(key, val)
		if err != nil {
			return false, err
		}
		fields = append(fields, fieldRow.Name())
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return fields, nil
}

func (r *firestormReader) GetInternal(key []byte) ([]byte, error) {
	internalRow := NewInternalRow(key, nil)
	return r.r.Get(internalRow.Key())
}

func (r *firestormReader) DocCount() uint64 {
	return r.docCount
}

func (r *firestormReader) Close() error {
	return r.r.Close()
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			// didn't overflow, so stop
			break
		}
	}
	return rv
}
