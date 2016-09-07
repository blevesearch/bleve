//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package smolder

import (
	"fmt"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type IndexReader struct {
	index    *SmolderingCouch
	kvreader store.KVReader
	docCount uint64
}

func (i *IndexReader) TermFieldReader(term []byte, fieldName string, includeFreq, includeNorm, includeTermVectors bool) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newSmolderingCouchTermFieldReader(i, term, uint16(fieldIndex), includeFreq, includeNorm, includeTermVectors)
	}
	return newSmolderingCouchTermFieldReader(i, []byte{ByteSeparator}, ^uint16(0), includeFreq, includeNorm, includeTermVectors)
}

func (i *IndexReader) FieldDict(fieldName string) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, nil, nil)
}

func (i *IndexReader) FieldDictRange(fieldName string, startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	fieldIndex, fieldExists := i.index.fieldCache.FieldNamed(fieldName, false)
	if fieldExists {
		return newSmolderingCouchFieldDict(i, uint16(fieldIndex), startTerm, endTerm)
	}
	return newSmolderingCouchFieldDict(i, ^uint16(0), []byte{ByteSeparator}, []byte{})
}

func (i *IndexReader) FieldDictPrefix(fieldName string, termPrefix []byte) (index.FieldDict, error) {
	return i.FieldDictRange(fieldName, termPrefix, termPrefix)
}

func (i *IndexReader) DocIDReader(start, end string) (index.DocIDReader, error) {
	return newSmolderingCouchDocIDReader(i, start, end)
}

func (i *IndexReader) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	return newSmolderingCouchDocIDReaderOnly(i, ids)
}

func (i *IndexReader) Document(id string) (doc *document.Document, err error) {

	// first hit the back index to confirm doc exists
	var backIndexRow *BackIndexRow
	backIndexRow, err = i.index.backIndexRowForDoc(i, nil, id)
	if err != nil {
		return
	}
	if backIndexRow == nil {
		return
	}
	doc = document.NewDocument(id)
	storedRowScanPrefix := NewStoredRowDocBytes(backIndexRow.docNumber, 0, []uint64{}, 'x', nil).ScanPrefixForDoc()
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

func (i *IndexReader) DocumentFieldTerms(id index.IndexInternalID) (index.FieldTerms, error) {
	back, err := i.index.backIndexRowForDoc(i, id, "")
	if err != nil {
		return nil, err
	}
	if back == nil {
		return nil, nil
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

func (i *IndexReader) DocumentFieldTermsForFields(id index.IndexInternalID, fields []string) (index.FieldTerms, error) {
	back, err := i.index.backIndexRowForDoc(i, id, "")
	if err != nil {
		return nil, err
	}
	rv := make(index.FieldTerms, len(fields))
	fieldsMap := make(map[uint16]string, len(fields))
	for _, f := range fields {
		id, ok := i.index.fieldCache.FieldNamed(f, false)
		if !ok {
			return nil, fmt.Errorf("Field %s was not found in cache", f)
		}
		fieldsMap[id] = f
	}
	for _, entry := range back.termEntries {
		if field, ok := fieldsMap[uint16(*entry.Field)]; ok {
			terms, ok := rv[field]
			if !ok {
				terms = make([]string, 0)
			}
			terms = append(terms, *entry.Term)
			rv[field] = terms
		}
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
		var row SmolderingCouchRow
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

func (i *IndexReader) ExternalID(id index.IndexInternalID) (string, error) {
	k := StoredRowDocFieldKey(id, 0)
	v, err := i.kvreader.Get(k)
	if err != nil {
		return "", err
	}
	return string(v[1:]), nil
}

func (i *IndexReader) InternalID(id string) (index.IndexInternalID, error) {
	tfr, err := i.TermFieldReader([]byte(id), "_id", false, false, false)
	if err != nil {
		return nil, err
	}
	if tfr.Count() < 1 {
		return nil, nil
	}
	tfd := index.TermFieldDoc{}
	tfr.Next(&tfd)
	return tfd.ID, nil
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
