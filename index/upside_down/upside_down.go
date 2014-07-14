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
	"sort"

	"github.com/couchbaselabs/bleve/analysis"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
	"github.com/couchbaselabs/bleve/index/store"
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

type termMap map[string]bool
type fieldTermMap map[int]termMap

func (udc *UpsideDownCouch) Update(doc *document.Document) error {
	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(doc.ID)
	if err != nil {
		return err
	}

	var isAdd = true
	// a map for each field, map key is term (string) bool true for existence
	existingTermFieldMaps := make(fieldTermMap, 0)
	if backIndexRow != nil {
		isAdd = false
		for _, entry := range backIndexRow.entries {
			existingTermMap, fieldExists := existingTermFieldMaps[int(entry.field)]
			if !fieldExists {
				existingTermMap = make(termMap, 0)
				existingTermFieldMaps[int(entry.field)] = existingTermMap
			}
			existingTermMap[string(entry.term)] = true
		}
	}
	existingStoredFieldMap := make(map[uint16]bool)
	if backIndexRow != nil {
		for _, sf := range backIndexRow.storedFields {
			existingStoredFieldMap[sf] = true
		}
	}

	// prepare a list of rows
	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)

	// track our back index entries
	backIndexEntries := make([]*BackIndexEntry, 0)
	backIndexStoredFields := make([]uint16, 0)

	for _, field := range doc.Fields {
		fieldIndex, fieldExists := udc.fieldIndexes[field.Name()]
		if !fieldExists {
			// assign next field id
			fieldIndex = uint16(udc.lastFieldIndex + 1)
			udc.fieldIndexes[field.Name()] = fieldIndex
			// ensure this batch adds a row for this field
			row := NewFieldRow(uint16(fieldIndex), field.Name())
			updateRows = append(updateRows, row)
			udc.lastFieldIndex = int(fieldIndex)
		}

		existingTermMap, fieldExistedInDoc := existingTermFieldMaps[int(fieldIndex)]

		if field.Options().IsIndexed() {

			analyzer := field.Analyzer()
			tokens := analyzer.Analyze(field.Value())
			fieldLength := len(tokens) // number of tokens in this doc field
			fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))
			tokenFreqs := analysis.TokenFrequency(tokens)
			for _, tf := range tokenFreqs {
				var termFreqRow *TermFrequencyRow
				if field.Options().IncludeTermVectors() {
					tv := termVectorsFromTokenFreq(uint16(fieldIndex), tf)
					termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, uint16(fieldIndex), doc.ID, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
				} else {
					termFreqRow = NewTermFrequencyRow(tf.Term, uint16(fieldIndex), doc.ID, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
				}

				// record the back index entry
				backIndexEntry := BackIndexEntry{tf.Term, uint16(fieldIndex)}
				backIndexEntries = append(backIndexEntries, &backIndexEntry)

				// remove the entry from the map of existing term fields if it exists
				if fieldExistedInDoc {
					termString := string(tf.Term)
					_, ok := existingTermMap[termString]
					if ok {
						// this is an update
						updateRows = append(updateRows, termFreqRow)
						// this term existed last time, delete it from that map
						delete(existingTermMap, termString)
					} else {
						// this is an add
						addRows = append(addRows, termFreqRow)
					}
				} else {
					// this is an add
					addRows = append(addRows, termFreqRow)
				}
			}
		}

		if field.Options().IsStored() {
			storedRow := NewStoredRow(doc.ID, uint16(fieldIndex), field.Value())
			backIndexStoredFields = append(backIndexStoredFields, fieldIndex)
			_, ok := existingStoredFieldMap[uint16(fieldIndex)]
			if ok {
				// this is an update
				updateRows = append(updateRows, storedRow)
				// this field was stored last time, delete it from that map
				delete(existingStoredFieldMap, uint16(fieldIndex))
			} else {
				addRows = append(addRows, storedRow)
			}
		}

	}

	// build the back index row
	backIndexRow = NewBackIndexRow(doc.ID, backIndexEntries, backIndexStoredFields)
	updateRows = append(updateRows, backIndexRow)

	// any of the existing rows that weren't updated need to be deleted
	deleteRows := make([]UpsideDownCouchRow, 0)
	for fieldIndex, existingTermFieldMap := range existingTermFieldMaps {
		if existingTermFieldMap != nil {
			for termString, _ := range existingTermFieldMap {
				termFreqRow := NewTermFrequencyRow([]byte(termString), uint16(fieldIndex), doc.ID, 0, 0)
				deleteRows = append(deleteRows, termFreqRow)
			}
		}
	}
	// any of the existing stored fields that weren't updated need to be deleted
	for storedFieldIndex, _ := range existingStoredFieldMap {
		storedRow := NewStoredRow(doc.ID, storedFieldIndex, nil)
		deleteRows = append(deleteRows, storedRow)
	}

	err = udc.batchRows(addRows, updateRows, deleteRows)
	if err == nil && isAdd {
		udc.docCount += 1
	}
	return err
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

	// prepare a list of rows to delete
	rows := make([]UpsideDownCouchRow, 0)
	for _, backIndexEntry := range backIndexRow.entries {
		tfr := NewTermFrequencyRow(backIndexEntry.term, backIndexEntry.field, id, 0, 0)
		rows = append(rows, tfr)
	}
	for _, sf := range backIndexRow.storedFields {
		sf := NewStoredRow(id, sf, nil)
		rows = append(rows, sf)
	}

	// also delete the back entry itself
	rows = append(rows, backIndexRow)

	err = udc.batchRows(nil, nil, rows)
	if err == nil {
		udc.docCount -= 1
	}
	return err
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

func (udc *UpsideDownCouch) Dump() {
	it := udc.store.Iterator([]byte{0})
	defer it.Close()
	key, val, valid := it.Current()
	for valid {

		row, err := ParseFromKeyValue(key, val)
		if err != nil {
			fmt.Printf("error parsing key/value: %v", err)
			return
		}
		if row != nil {
			fmt.Printf("%v\n", row)
			fmt.Printf("Key:   % -100x\nValue: % -100x\n\n", key, val)
		}

		it.Next()
		key, val, valid = it.Current()
	}
}

type keyset [][]byte

func (k keyset) Len() int           { return len(k) }
func (k keyset) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k keyset) Less(i, j int) bool { return bytes.Compare(k[i], k[j]) < 0 }

// DumpDoc returns all rows in the index related to this doc id
func (udc *UpsideDownCouch) DumpDoc(id string) ([]interface{}, error) {
	rv := make([]interface{}, 0)
	back, err := udc.backIndexRowForDoc(id)
	if err != nil {
		return nil, err
	}
	keys := make(keyset, 0)
	for _, stored := range back.storedFields {
		sr := NewStoredRow(id, stored, []byte{})
		key := sr.Key()
		keys = append(keys, key)
	}
	for _, entry := range back.entries {
		//log.Printf("term: `%s`, field: %d", entry.term, entry.field)
		tfr := NewTermFrequencyRow(entry.term, entry.field, id, 0, 0)
		key := tfr.Key()
		keys = append(keys, key)
	}
	sort.Sort(keys)

	for _, key := range keys {
		value, err := udc.store.Get(key)
		if err != nil {
			return nil, err
		}
		row, err := ParseFromKeyValue(key, value)
		if err != nil {
			return nil, err
		}
		rv = append(rv, row)
	}

	return rv, nil
}

func (udc *UpsideDownCouch) TermFieldReader(term []byte, fieldName string) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := udc.fieldIndexes[fieldName]
	if fieldExists {
		return newUpsideDownCouchTermFieldReader(udc, term, uint16(fieldIndex))
	}
	return newUpsideDownCouchTermFieldReader(udc, []byte{BYTE_SEPARATOR}, 0)
}

func (udc *UpsideDownCouch) DocIdReader(start, end string) (index.DocIdReader, error) {
	return newUpsideDownCouchDocIdReader(udc, start, end)
}

func (udc *UpsideDownCouch) Document(id string) (*document.Document, error) {
	rv := document.NewDocument(id)
	storedRow := NewStoredRow(id, 0, nil)
	storedRowScanPrefix := storedRow.ScanPrefixForDoc()
	it := udc.store.Iterator(storedRowScanPrefix)
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
			rv.AddField(document.NewTextField(udc.fieldIndexToName(row.field), row.Value()))
		}

		it.Next()
		key, val, valid = it.Current()
	}
	return rv, nil
}

func frequencyFromTokenFreq(tf *analysis.TokenFreq) int {
	return len(tf.Locations)
}

func termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq) []*TermVector {
	rv := make([]*TermVector, len(tf.Locations))

	for i, l := range tf.Locations {
		tv := TermVector{
			field: field,
			pos:   uint64(l.Position),
			start: uint64(l.Start),
			end:   uint64(l.End),
		}
		rv[i] = &tv
	}

	return rv
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
