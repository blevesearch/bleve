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
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newUpsideDownCouchTermFieldReader(i, term, uint16(fieldIndex))
	}
	return newUpsideDownCouchTermFieldReader(i, []byte{ByteSeparator}, ^uint16(0))
}

func (i *IndexReader) FieldDict(fieldName string) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, nil, nil)
}

func (i *IndexReader) FieldDictRange(fieldName string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newUpsideDownCouchFieldDict(i, uint16(fieldIndex), startTerm, endTerm)
	}
	return newUpsideDownCouchFieldDict(i, ^uint16(0), []byte{ByteSeparator}, []byte{})
}

func (i *IndexReader) FieldDictPrefix(fieldName string, termPrefix []byte) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, termPrefix, termPrefix)
}

func (i *IndexReader) DocIDReader(start, end string) (index.DocIDReader, error) {
	return newUpsideDownCouchDocIDReader(i, start, end)
}

func (i *IndexReader) Document(id string) (doc *document.Document, err error) {
	// first hit the back index to confirm doc exists
	var backIndexRow *BackIndexRow
	backIndexRow, err = i.index.backIndexRowForDoc(i.kvreader, id)
	if err != nil {
		return
	}
	if backIndexRow == nil {
		return
	}
	doc = document.NewDocument(id)
	storedRow := NewStoredRow([]byte(id), 0, []uint64{}, 'x', nil)
	storedRowScanPrefix := storedRow.ScanPrefixForDoc()
	it := i.kvreader.PrefixIterator(storedRowScanPrefix)
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	key, val, valid := it.Current()
	for valid {
		safeVal := make([]byte, len(val))
		copy(safeVal, val)
		var row *StoredRow
		row, err = NewStoredRowKV(key, safeVal)
		if err != nil {
			doc = nil
			return
		}
		if row != nil {
			fieldName := i.index.fieldCache.FieldIndexed(row.field)
			field := decodeFieldType(row.typ, fieldName, row.arrayPositions, row.value)
			if field != nil {
				doc.AddField(field)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}
	return
}

func (i *IndexReader) DocumentFieldTerms(id string) (index.FieldTerms, error) {
	back, err := i.index.backIndexRowForDoc(i.kvreader, id)
	if err != nil {
		return nil, err
	}
	rv := make(index.FieldTerms, len(back.termEntries))
	for _, entry := range back.termEntries {
		fieldName := i.index.fieldCache.FieldIndexed(uint16(*entry.Field))
		terms, ok := rv[fieldName]
		if !ok {
			terms = make([]string, 0)
		}
		terms = append(terms, *entry.Term)
		rv[fieldName] = terms
	}
	return rv, nil
}

func (i *IndexReader) Fields() (fields []string, err error) {
	fields = make([]string, 0)
	it := i.kvreader.PrefixIterator([]byte{'f'})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	key, val, valid := it.Current()
	for valid {
		var row UpsideDownCouchRow
		row, err = ParseFromKeyValue(key, val)
		if err != nil {
			fields = nil
			return
		}
		if row != nil {
			fieldRow, ok := row.(*FieldRow)
			if ok {
				fields = append(fields, fieldRow.name)
			}
		}

		it.Next()
		key, val, valid = it.Current()
	}
	return
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
