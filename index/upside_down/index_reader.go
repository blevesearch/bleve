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

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type IndexReader struct {
	index    *UpsideDownCouch
	kvreader store.KVReader
	docCount uint64
}

func (i *IndexReader) TermFieldReader(term []byte, fieldName string) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := i.index.fieldIndexCache.FieldExists(fieldName)
	if fieldExists {
		return newUpsideDownCouchTermFieldReader(i, term, uint16(fieldIndex))
	}
	return newUpsideDownCouchTermFieldReader(i, []byte{ByteSeparator}, ^uint16(0))
}

func (i *IndexReader) FieldDict(fieldName string) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, nil, nil)
}

func (i *IndexReader) FieldDictRange(fieldName string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	fieldIndex, fieldExists := i.index.fieldIndexCache.FieldExists(fieldName)
	if fieldExists {
		return newUpsideDownCouchFieldDict(i, uint16(fieldIndex), startTerm, endTerm)
	}
	return newUpsideDownCouchFieldDict(i, ^uint16(0), []byte{ByteSeparator}, []byte{})
}

func (i *IndexReader) FieldDictPrefix(fieldName string, termPrefix []byte) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, termPrefix, incrementBytes(termPrefix))
}

func (i *IndexReader) DocIDReader(start, end string) (index.DocIDReader, error) {
	return newUpsideDownCouchDocIDReader(i, start, end)
}

func (i *IndexReader) Document(id string) (*document.Document, error) {
	// first hit the back index to confirm doc exists
	backIndexRow, err := i.index.backIndexRowForDoc(i.kvreader, id)
	if err != nil {
		return nil, err
	}
	if backIndexRow == nil {
		return nil, nil
	}
	rv := document.NewDocument(id)
	storedRow := NewStoredRow(id, 0, []uint64{}, 'x', nil)
	storedRowScanPrefix := storedRow.ScanPrefixForDoc()
	it := i.kvreader.Iterator(storedRowScanPrefix)
	defer it.Close()
	key, val, valid := it.Current()
	for valid {
		if !bytes.HasPrefix(key, storedRowScanPrefix) {
			break
		}
		row, err := NewStoredRowKV(key, val)
		if err != nil {
			return nil, err
		}
		if row != nil {
			fieldName := i.index.fieldIndexCache.FieldName(row.field)
			field := decodeFieldType(row.typ, fieldName, row.value)
			if field != nil {
				rv.AddField(field)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}
	return rv, nil
}

func (i *IndexReader) DocumentFieldTerms(id string) (index.FieldTerms, error) {
	back, err := i.index.backIndexRowForDoc(i.kvreader, id)
	if err != nil {
		return nil, err
	}
	rv := make(index.FieldTerms, len(back.termEntries))
	for _, entry := range back.termEntries {
		fieldName := i.index.fieldIndexCache.FieldName(uint16(*entry.Field))
		terms, ok := rv[fieldName]
		if !ok {
			terms = make([]string, 0)
		}
		terms = append(terms, *entry.Term)
		rv[fieldName] = terms
	}
	return rv, nil
}

func (i *IndexReader) Fields() ([]string, error) {
	rv := make([]string, 0)
	it := i.kvreader.Iterator([]byte{'f'})
	defer it.Close()
	key, val, valid := it.Current()
	for valid {
		if !bytes.HasPrefix(key, []byte{'f'}) {
			break
		}
		row, err := ParseFromKeyValue(key, val)
		if err != nil {
			return nil, err
		}
		if row != nil {
			fieldRow, ok := row.(*FieldRow)
			if ok {
				rv = append(rv, fieldRow.name)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}
	return rv, nil
}

func (i *IndexReader) GetInternal(key []byte) ([]byte, error) {
	internalRow := NewInternalRow(key, nil)
	return i.kvreader.Get(internalRow.Key())
}

func (i *IndexReader) DocCount() uint64 {
	return i.docCount
}

func (i *IndexReader) Close() error {
	return i.kvreader.Close()
}
