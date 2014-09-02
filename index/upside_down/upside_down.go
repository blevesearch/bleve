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
	"fmt"
	"math"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"

	"code.google.com/p/goprotobuf/proto"
)

var VERSION_KEY []byte = []byte{'v'}

const VERSION uint8 = 1

type UpsideDownCouch struct {
	version        uint8
	path           string
	store          store.KVStore
	fieldIndexes   map[string]uint16
	lastFieldIndex int
	analyzer       map[string]*analysis.Analyzer
	docCount       uint64
}

func NewUpsideDownCouch(s store.KVStore) *UpsideDownCouch {
	return &UpsideDownCouch{
		version:      VERSION,
		analyzer:     make(map[string]*analysis.Analyzer),
		fieldIndexes: make(map[string]uint16),
		store:        s,
	}
}

func (udc *UpsideDownCouch) init() (err error) {
	// prepare a list of rows
	rows := make([]UpsideDownCouchRow, 0)

	// version marker
	rows = append(rows, NewVersionRow(udc.version))

	return udc.batchRows(nil, rows, nil)
}

func (udc *UpsideDownCouch) loadSchema() (err error) {

	keyPrefix := []byte{'f'}
	it := udc.store.Iterator(keyPrefix)
	defer it.Close()

	it.Seek(keyPrefix)
	key, val, valid := it.Current()
	for valid {

		// stop when
		if !bytes.HasPrefix(key, keyPrefix) {
			break
		}
		fieldRow, err := NewFieldRowKV(key, val)
		if err != nil {
			return err
		}
		udc.fieldIndexes[fieldRow.name] = fieldRow.index
		if int(fieldRow.index) > udc.lastFieldIndex {
			udc.lastFieldIndex = int(fieldRow.index)
		}

		it.Next()
		key, val, valid = it.Current()
	}

	return
}

func (udc *UpsideDownCouch) batchRows(addRows []UpsideDownCouchRow, updateRows []UpsideDownCouchRow, deleteRows []UpsideDownCouchRow) (err error) {

	// prepare batch
	wb := udc.store.NewBatch()

	// add
	for _, row := range addRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to increment counter
			tr := NewTermFrequencyRow(tfr.term, tfr.field, "", 0, 0)
			val, err := udc.store.Get(tr.Key())
			if err != nil {
				return err
			}
			if val != nil {
				tr, err = NewTermFrequencyRowKV(tr.Key(), val)
				if err != nil {
					return err
				}
				tr.freq += 1 // incr
			} else {
				tr = NewTermFrequencyRow(tfr.term, tfr.field, "", 1, 0)
			}

			// now add this to the batch
			wb.Set(tr.Key(), tr.Value())
		}
		wb.Set(row.Key(), row.Value())
	}

	// update
	for _, row := range updateRows {
		wb.Set(row.Key(), row.Value())
	}

	// delete
	for _, row := range deleteRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to decrement counter
			tr := NewTermFrequencyRow(tfr.term, tfr.field, "", 0, 0)
			val, err := udc.store.Get(tr.Key())
			if err != nil {
				return err
			}
			if val != nil {
				tr, err = NewTermFrequencyRowKV(tr.Key(), val)
				if err != nil {
					return err
				}
				tr.freq -= 1 // incr
			} else {
				return fmt.Errorf("unexpected missing row, deleting term, expected count row to exist: %v", tr.Key())
			}

			if tr.freq == 0 {
				wb.Delete(tr.Key())
			} else {
				// now add this to the batch
				wb.Set(tr.Key(), tr.Value())
			}

		}
		wb.Delete(row.Key())
	}

	// write out the batch
	err = wb.Execute()
	if err != nil {
		return
	}
	err = udc.store.Commit()
	return
}

func (udc *UpsideDownCouch) DocCount() uint64 {
	return udc.docCount
}

func (udc *UpsideDownCouch) Open() (err error) {
	var value []byte
	value, err = udc.store.Get(VERSION_KEY)
	if err != nil {
		return
	}

	// init new index OR load schema
	if value == nil {
		err = udc.init()
		if err != nil {
			return
		}
	} else {
		err = udc.loadSchema()
		if err != nil {
			return
		}
	}
	// set doc count
	udc.docCount = udc.countDocs()
	return
}

func (udc *UpsideDownCouch) countDocs() uint64 {
	it := udc.store.Iterator([]byte{'b'})
	defer it.Close()

	var rv uint64 = 0
	key, _, valid := it.Current()
	for valid {
		if !bytes.HasPrefix(key, []byte{'b'}) {
			break
		}
		rv += 1
		it.Next()
		key, _, valid = it.Current()
	}

	return rv
}

func (udc *UpsideDownCouch) rowCount() uint64 {
	it := udc.store.Iterator([]byte{0})
	defer it.Close()

	var rv uint64 = 0
	_, _, valid := it.Current()
	for valid {
		rv += 1
		it.Next()
		_, _, valid = it.Current()
	}

	return rv
}

func (udc *UpsideDownCouch) Close() {
	udc.store.Close()
}

func (udc *UpsideDownCouch) Update(doc *document.Document) error {
	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(doc.ID)
	if err != nil {
		return err
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	addRows, updateRows, deleteRows = udc.updateSingle(doc, backIndexRow, addRows, updateRows, deleteRows)

	err = udc.batchRows(addRows, updateRows, deleteRows)
	if err == nil && backIndexRow == nil {
		udc.docCount += 1
	}
	return err
}

func (udc *UpsideDownCouch) updateSingle(doc *document.Document, backIndexRow *BackIndexRow, addRows, updateRows, deleteRows []UpsideDownCouchRow) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []UpsideDownCouchRow) {

	existingTermKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllTermKeys() {
		existingTermKeys[string(key)] = true
	}

	existingStoredKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllStoredKeys() {
		existingStoredKeys[string(key)] = true
	}

	// track our back index entries
	backIndexTermEntries := make([]*BackIndexTermEntry, 0)
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)

	for _, field := range doc.Fields {
		fieldIndex, newFieldRow := udc.fieldNameToFieldIndex(field.Name())
		if newFieldRow != nil {
			updateRows = append(updateRows, newFieldRow)
		}

		if field.Options().IsIndexed() {

			fieldLength, tokenFreqs := field.Analyze()

			// see if any of the composite fields need this
			for _, compositeField := range doc.CompositeFields {
				compositeField.Compose(field.Name(), fieldLength, tokenFreqs)
			}

			// encode this field
			indexAddRows, indexUpdateRows, indexBackIndexTermEntries := udc.indexField(doc.ID, field, fieldIndex, fieldLength, tokenFreqs, existingTermKeys)
			addRows = append(addRows, indexAddRows...)
			updateRows = append(updateRows, indexUpdateRows...)
			backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
		}

		if field.Options().IsStored() {
			storeAddRows, storeUpdateRows, indexBackIndexStoreEntries := udc.storeField(doc.ID, field, fieldIndex, existingStoredKeys)
			addRows = append(addRows, storeAddRows...)
			updateRows = append(updateRows, storeUpdateRows...)
			backIndexStoredEntries = append(backIndexStoredEntries, indexBackIndexStoreEntries...)
		}

	}

	// now index the composite fields
	for _, compositeField := range doc.CompositeFields {
		fieldIndex, newFieldRow := udc.fieldNameToFieldIndex(compositeField.Name())
		if newFieldRow != nil {
			updateRows = append(updateRows, newFieldRow)
		}
		if compositeField.Options().IsIndexed() {

			fieldLength, tokenFreqs := compositeField.Analyze()
			// encode this field
			indexAddRows, indexUpdateRows, indexBackIndexTermEntries := udc.indexField(doc.ID, compositeField, fieldIndex, fieldLength, tokenFreqs, existingTermKeys)
			addRows = append(addRows, indexAddRows...)
			updateRows = append(updateRows, indexUpdateRows...)
			backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
		}
	}

	// build the back index row
	backIndexRow = NewBackIndexRow(doc.ID, backIndexTermEntries, backIndexStoredEntries)
	updateRows = append(updateRows, backIndexRow)

	// any of the existing rows that weren't updated need to be deleted
	for existingTermKey, _ := range existingTermKeys {
		termFreqRow, err := NewTermFrequencyRowK([]byte(existingTermKey))
		if err == nil {
			deleteRows = append(deleteRows, termFreqRow)
		}
	}

	// any of the existing stored fields that weren't updated need to be deleted
	for existingStoredKey, _ := range existingStoredKeys {
		storedRow, err := NewStoredRowK([]byte(existingStoredKey))
		if err == nil {
			deleteRows = append(deleteRows, storedRow)
		}
	}

	return addRows, updateRows, deleteRows
}

func (udc *UpsideDownCouch) storeField(docId string, field document.Field, fieldIndex uint16, existingKeys map[string]bool) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []*BackIndexStoreEntry) {
	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docId, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

	// record the back index entry
	backIndexStoredEntry := BackIndexStoreEntry{Field: proto.Uint32(uint32(fieldIndex)), ArrayPositions: field.ArrayPositions()}
	backIndexStoredEntries = append(backIndexStoredEntries, &backIndexStoredEntry)

	storedRowKey := string(storedRow.Key())
	_, existed := existingKeys[storedRowKey]
	if existed {
		// this is an update
		updateRows = append(updateRows, storedRow)
		// this field was stored last time, delete it from that map
		delete(existingKeys, storedRowKey)
	} else {
		addRows = append(addRows, storedRow)
	}
	return addRows, updateRows, backIndexStoredEntries
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}

func (udc *UpsideDownCouch) indexField(docId string, field document.Field, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies, existingKeys map[string]bool) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []*BackIndexTermEntry) {

	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)
	backIndexTermEntries := make([]*BackIndexTermEntry, 0)
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for _, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if field.Options().IncludeTermVectors() {
			tv, newFieldRows := udc.termVectorsFromTokenFreq(fieldIndex, tf)
			updateRows = append(updateRows, newFieldRows...)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docId, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docId, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		backIndexTermEntry := BackIndexTermEntry{Term: proto.String(string(tf.Term)), Field: proto.Uint32(uint32(fieldIndex))}
		backIndexTermEntries = append(backIndexTermEntries, &backIndexTermEntry)

		tfrKeyString := string(termFreqRow.Key())
		_, existed := existingKeys[tfrKeyString]
		if existed {
			// this is an update
			updateRows = append(updateRows, termFreqRow)
			// this term existed last time, delete it from that map
			delete(existingKeys, tfrKeyString)
		} else {
			// this is an add
			addRows = append(addRows, termFreqRow)
		}
	}

	return addRows, updateRows, backIndexTermEntries
}

func (udc *UpsideDownCouch) fieldNameToFieldIndex(fieldName string) (uint16, *FieldRow) {
	var fieldRow *FieldRow
	fieldIndex, fieldExists := udc.fieldIndexes[fieldName]
	if !fieldExists {
		// assign next field id
		fieldIndex = uint16(udc.lastFieldIndex + 1)
		udc.fieldIndexes[fieldName] = fieldIndex
		// ensure this batch adds a row for this field
		fieldRow = NewFieldRow(uint16(fieldIndex), fieldName)
		udc.lastFieldIndex = int(fieldIndex)
	}
	return fieldIndex, fieldRow
}

func (udc *UpsideDownCouch) Delete(id string) error {
	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(id)
	if err != nil {
		return err
	}
	if backIndexRow == nil {
		return nil
	}

	deleteRows := make([]UpsideDownCouchRow, 0)
	deleteRows = udc.deleteSingle(id, backIndexRow, deleteRows)

	err = udc.batchRows(nil, nil, deleteRows)
	if err == nil {
		udc.docCount -= 1
	}
	return err
}

func (udc *UpsideDownCouch) deleteSingle(id string, backIndexRow *BackIndexRow, deleteRows []UpsideDownCouchRow) []UpsideDownCouchRow {

	for _, backIndexEntry := range backIndexRow.termEntries {
		tfr := NewTermFrequencyRow([]byte(*backIndexEntry.Term), uint16(*backIndexEntry.Field), id, 0, 0)
		deleteRows = append(deleteRows, tfr)
	}
	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRow(id, uint16(*se.Field), se.ArrayPositions, 'x', nil)
		deleteRows = append(deleteRows, sf)
	}

	// also delete the back entry itself
	deleteRows = append(deleteRows, backIndexRow)
	return deleteRows
}

func (udc *UpsideDownCouch) backIndexRowForDoc(docId string) (*BackIndexRow, error) {
	// use a temporary row structure to build key
	tempRow := &BackIndexRow{
		doc: []byte(docId),
	}
	key := tempRow.Key()
	value, err := udc.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	backIndexRow, err := NewBackIndexRowKV(key, value)
	if err != nil {
		return nil, err
	}
	return backIndexRow, nil
}

func (udc *UpsideDownCouch) backIndexRowsForBatch(batch index.Batch) (map[string]*BackIndexRow, error) {
	// FIXME faster to order the ids and scan sequentially
	// for now just get it working
	rv := make(map[string]*BackIndexRow, 0)
	for docId, _ := range batch {
		backIndexRow, err := udc.backIndexRowForDoc(docId)
		if err != nil {
			return nil, err
		}
		rv[docId] = backIndexRow
	}
	return rv, nil
}

func (udc *UpsideDownCouch) Fields() ([]string, error) {
	rv := make([]string, 0)
	it := udc.store.Iterator([]byte{'f'})
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

func (udc *UpsideDownCouch) TermFieldReader(term []byte, fieldName string) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := udc.fieldIndexes[fieldName]
	if fieldExists {
		return newUpsideDownCouchTermFieldReader(udc, term, uint16(fieldIndex))
	}
	return newUpsideDownCouchTermFieldReader(udc, []byte{BYTE_SEPARATOR}, ^uint16(0))
}

func (udc *UpsideDownCouch) FieldReader(fieldName string, startTerm []byte, endTerm []byte) (index.FieldReader, error) {
	fieldIndex, fieldExists := udc.fieldIndexes[fieldName]
	if fieldExists {
		return newUpsideDownCouchFieldReader(udc, uint16(fieldIndex), startTerm, endTerm)
	}
	return newUpsideDownCouchTermFieldReader(udc, []byte{BYTE_SEPARATOR}, ^uint16(0))
}

func (udc *UpsideDownCouch) DocIdReader(start, end string) (index.DocIdReader, error) {
	return newUpsideDownCouchDocIdReader(udc, start, end)
}

func (udc *UpsideDownCouch) Document(id string) (*document.Document, error) {
	// first hit the back index to confirm doc exists
	backIndexRow, err := udc.backIndexRowForDoc(id)
	if err != nil {
		return nil, err
	}
	if backIndexRow == nil {
		return nil, nil
	}
	rv := document.NewDocument(id)
	storedRow := NewStoredRow(id, 0, []uint64{}, 'x', nil)
	storedRowScanPrefix := storedRow.ScanPrefixForDoc()
	it := udc.store.Iterator(storedRowScanPrefix)
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
			fieldName := udc.fieldIndexToName(row.field)
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

func (udc *UpsideDownCouch) DocumentFieldTerms(id string) (index.FieldTerms, error) {
	back, err := udc.backIndexRowForDoc(id)
	if err != nil {
		return nil, err
	}
	rv := make(index.FieldTerms, len(back.termEntries))
	for _, entry := range back.termEntries {
		fieldName := udc.fieldIndexToName(uint16(*entry.Field))
		terms, ok := rv[fieldName]
		if !ok {
			terms = make([]string, 0)
		}
		terms = append(terms, *entry.Term)
		rv[fieldName] = terms
	}
	return rv, nil
}

func decodeFieldType(typ byte, name string, value []byte) document.Field {
	switch typ {
	case 't':
		return document.NewTextField(name, []uint64{}, value)
	case 'n':
		return document.NewNumericFieldFromBytes(name, []uint64{}, value)
	case 'd':
		return document.NewDateTimeFieldFromBytes(name, []uint64{}, value)
	}
	return nil
}

func frequencyFromTokenFreq(tf *analysis.TokenFreq) int {
	return len(tf.Locations)
}

func (udc *UpsideDownCouch) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq) ([]*TermVector, []UpsideDownCouchRow) {
	rv := make([]*TermVector, len(tf.Locations))
	newFieldRows := make([]UpsideDownCouchRow, 0)

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = udc.fieldNameToFieldIndex(l.Field)
			if newFieldRow != nil {
				newFieldRows = append(newFieldRows, newFieldRow)
			}
		}
		tv := TermVector{
			field: fieldIndex,
			pos:   uint64(l.Position),
			start: uint64(l.Start),
			end:   uint64(l.End),
		}
		rv[i] = &tv
	}

	return rv, newFieldRows
}

func (udc *UpsideDownCouch) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	rv := make([]*index.TermFieldVector, len(in))

	for i, tv := range in {
		fieldName := udc.fieldIndexToName(tv.field)
		tfv := index.TermFieldVector{
			Field: fieldName,
			Pos:   tv.pos,
			Start: tv.start,
			End:   tv.end,
		}
		rv[i] = &tfv
	}
	return rv
}

func (udc *UpsideDownCouch) fieldIndexToName(i uint16) string {
	for fieldName, fieldIndex := range udc.fieldIndexes {
		if i == fieldIndex {
			return fieldName
		}
	}
	return ""
}

func (udc *UpsideDownCouch) Batch(batch index.Batch) error {
	// first lookup all the back index rows
	backIndexRows, err := udc.backIndexRowsForBatch(batch)
	if err != nil {
		return err
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	docsAdded := uint64(0)
	docsDeleted := uint64(0)
	for docId, doc := range batch {
		backIndexRow := backIndexRows[docId]
		if doc == nil && backIndexRow != nil {
			//delete
			deleteRows = udc.deleteSingle(docId, backIndexRow, deleteRows)
			docsDeleted++
		} else if doc != nil {
			addRows, updateRows, deleteRows = udc.updateSingle(doc, backIndexRow, addRows, updateRows, deleteRows)
			if backIndexRow == nil {
				docsAdded++
			}
		}
	}

	err = udc.batchRows(addRows, updateRows, deleteRows)
	if err == nil {
		udc.docCount += docsAdded
		udc.docCount -= docsDeleted
	}
	return err
}

func (udc *UpsideDownCouch) SetInternal(key, val []byte) error {
	internalRow := NewInternalRow(key, val)
	return udc.store.Set(internalRow.Key(), internalRow.Value())
}

func (udc *UpsideDownCouch) GetInternal(key []byte) ([]byte, error) {
	internalRow := NewInternalRow(key, nil)
	return udc.store.Get(internalRow.Key())
}

func (udc *UpsideDownCouch) DeleteInternal(key []byte) error {
	internalRow := NewInternalRow(key, nil)
	return udc.store.Delete(internalRow.Key())
}
