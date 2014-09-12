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
	"math"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"

	"code.google.com/p/goprotobuf/proto"
)

var VersionKey = []byte{'v'}

const Version uint8 = 1

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
		version:      Version,
		analyzer:     make(map[string]*analysis.Analyzer),
		fieldIndexes: make(map[string]uint16),
		store:        s,
	}
}

func (udc *UpsideDownCouch) init(kvwriter store.KVWriter) (err error) {
	// prepare a list of rows
	rows := make([]UpsideDownCouchRow, 0)

	// version marker
	rows = append(rows, NewVersionRow(udc.version))

	return udc.batchRows(kvwriter, nil, rows, nil)
}

func (udc *UpsideDownCouch) loadSchema(kvreader store.KVReader) (err error) {

	keyPrefix := []byte{'f'}
	it := kvreader.Iterator(keyPrefix)
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

func (udc *UpsideDownCouch) batchRows(writer store.KVWriter, addRows []UpsideDownCouchRow, updateRows []UpsideDownCouchRow, deleteRows []UpsideDownCouchRow) (err error) {

	// prepare batch
	wb := writer.NewBatch()

	// add
	for _, row := range addRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to increment counter
			summaryKey := tfr.SummaryKey()
			wb.Merge(summaryKey, newTermSummaryIncr())
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
			summaryKey := tfr.SummaryKey()
			wb.Merge(summaryKey, newTermSummaryDecr())
		}
		wb.Delete(row.Key())
	}

	// write out the batch
	err = wb.Execute()
	if err != nil {
		return
	}
	return
}

func (udc *UpsideDownCouch) DocCount() uint64 {
	return udc.docCount
}

func (udc *UpsideDownCouch) Open() error {
	// start a writer for the open process
	kvwriter := udc.store.Writer()
	defer kvwriter.Close()

	value, err := kvwriter.Get(VersionKey)
	if err != nil {
		return err
	}

	// init new index OR load schema
	if value == nil {
		err = udc.init(kvwriter)
		if err != nil {
			return err
		}
	} else {
		err = udc.loadSchema(kvwriter)
		if err != nil {
			return err
		}
	}
	// set doc count
	udc.docCount = udc.countDocs(kvwriter)
	return nil
}

func (udc *UpsideDownCouch) countDocs(kvreader store.KVReader) uint64 {
	it := kvreader.Iterator([]byte{'b'})
	defer it.Close()

	var rv uint64
	key, _, valid := it.Current()
	for valid {
		if !bytes.HasPrefix(key, []byte{'b'}) {
			break
		}
		rv++
		it.Next()
		key, _, valid = it.Current()
	}

	return rv
}

func (udc *UpsideDownCouch) rowCount() uint64 {
	// start an isolated reader for use during the rowcount
	kvreader := udc.store.Reader()
	defer kvreader.Close()
	it := kvreader.Iterator([]byte{0})
	defer it.Close()

	var rv uint64
	_, _, valid := it.Current()
	for valid {
		rv++
		it.Next()
		_, _, valid = it.Current()
	}

	return rv
}

func (udc *UpsideDownCouch) Close() {
	udc.store.Close()
}

func (udc *UpsideDownCouch) Update(doc *document.Document) error {
	// start a writer for this update
	kvwriter := udc.store.Writer()
	defer kvwriter.Close()

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(kvwriter, doc.ID)
	if err != nil {
		return err
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	addRows, updateRows, deleteRows = udc.updateSingle(doc, backIndexRow, addRows, updateRows, deleteRows)

	err = udc.batchRows(kvwriter, addRows, updateRows, deleteRows)
	if err == nil && backIndexRow == nil {
		udc.docCount++
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
	for existingTermKey := range existingTermKeys {
		termFreqRow, err := NewTermFrequencyRowK([]byte(existingTermKey))
		if err == nil {
			deleteRows = append(deleteRows, termFreqRow)
		}
	}

	// any of the existing stored fields that weren't updated need to be deleted
	for existingStoredKey := range existingStoredKeys {
		storedRow, err := NewStoredRowK([]byte(existingStoredKey))
		if err == nil {
			deleteRows = append(deleteRows, storedRow)
		}
	}

	return addRows, updateRows, deleteRows
}

func (udc *UpsideDownCouch) storeField(docID string, field document.Field, fieldIndex uint16, existingKeys map[string]bool) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []*BackIndexStoreEntry) {
	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docID, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

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

func (udc *UpsideDownCouch) indexField(docID string, field document.Field, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies, existingKeys map[string]bool) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []*BackIndexTermEntry) {

	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)
	backIndexTermEntries := make([]*BackIndexTermEntry, 0)
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for _, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if field.Options().IncludeTermVectors() {
			tv, newFieldRows := udc.termVectorsFromTokenFreq(fieldIndex, tf)
			updateRows = append(updateRows, newFieldRows...)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
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
	// start a writer for this delete
	kvwriter := udc.store.Writer()
	defer kvwriter.Close()

	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(kvwriter, id)
	if err != nil {
		return err
	}
	if backIndexRow == nil {
		return nil
	}

	deleteRows := make([]UpsideDownCouchRow, 0)
	deleteRows = udc.deleteSingle(id, backIndexRow, deleteRows)

	err = udc.batchRows(kvwriter, nil, nil, deleteRows)
	if err == nil {
		udc.docCount--
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

func (udc *UpsideDownCouch) backIndexRowForDoc(kvreader store.KVReader, docID string) (*BackIndexRow, error) {
	// use a temporary row structure to build key
	tempRow := &BackIndexRow{
		doc: []byte(docID),
	}
	key := tempRow.Key()
	value, err := kvreader.Get(key)
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

func (udc *UpsideDownCouch) backIndexRowsForBatch(kvreader store.KVReader, batch index.Batch) (map[string]*BackIndexRow, error) {
	// FIXME faster to order the ids and scan sequentially
	// for now just get it working
	rv := make(map[string]*BackIndexRow, 0)
	for docID := range batch {
		backIndexRow, err := udc.backIndexRowForDoc(kvreader, docID)
		if err != nil {
			return nil, err
		}
		rv[docID] = backIndexRow
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
	// start a writer for this batch
	kvwriter := udc.store.Writer()
	defer kvwriter.Close()

	// first lookup all the back index rows
	backIndexRows, err := udc.backIndexRowsForBatch(kvwriter, batch)
	if err != nil {
		return err
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	docsAdded := uint64(0)
	docsDeleted := uint64(0)
	for docID, doc := range batch {
		backIndexRow := backIndexRows[docID]
		if doc == nil && backIndexRow != nil {
			//delete
			deleteRows = udc.deleteSingle(docID, backIndexRow, deleteRows)
			docsDeleted++
		} else if doc != nil {
			addRows, updateRows, deleteRows = udc.updateSingle(doc, backIndexRow, addRows, updateRows, deleteRows)
			if backIndexRow == nil {
				docsAdded++
			}
		}
	}

	err = udc.batchRows(kvwriter, addRows, updateRows, deleteRows)
	if err == nil {
		udc.docCount += docsAdded
		udc.docCount -= docsDeleted
	}
	return err
}

func (udc *UpsideDownCouch) SetInternal(key, val []byte) error {
	internalRow := NewInternalRow(key, val)
	writer := udc.store.Writer()
	defer writer.Close()
	return writer.Set(internalRow.Key(), internalRow.Value())
}

func (udc *UpsideDownCouch) DeleteInternal(key []byte) error {
	internalRow := NewInternalRow(key, nil)
	writer := udc.store.Writer()
	defer writer.Close()
	return writer.Delete(internalRow.Key())
}

func (udc *UpsideDownCouch) Reader() index.IndexReader {
	return &IndexReader{
		index:    udc,
		kvreader: udc.store.Reader(),
		docCount: udc.docCount,
	}
}
