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
	"log"
	"math"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/jmhodges/levigo"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
)

var VERSION_KEY []byte = []byte{'v'}

const VERSION uint8 = 1

type UpsideDownCouch struct {
	version        uint8
	path           string
	opts           *levigo.Options
	db             *levigo.DB
	fieldIndexes   map[string]uint16
	lastFieldIndex int
	analyzer       map[string]*analysis.Analyzer
	docCount       uint64
}

func NewUpsideDownCouch(path string) *UpsideDownCouch {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)

	return &UpsideDownCouch{
		version:      VERSION,
		path:         path,
		opts:         opts,
		analyzer:     make(map[string]*analysis.Analyzer),
		fieldIndexes: make(map[string]uint16),
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
	// schema := make([]*index.Field, 0)

	ro := defaultReadOptions()
	it := udc.db.NewIterator(ro)
	defer it.Close()

	keyPrefix := []byte{'f'}
	it.Seek(keyPrefix)
	for it = it; it.Valid(); it.Next() {
		// stop when
		if !bytes.HasPrefix(it.Key(), keyPrefix) {
			break
		}
		fieldRow := NewFieldRowKV(it.Key(), it.Value())
		udc.fieldIndexes[fieldRow.name] = fieldRow.index
		if int(fieldRow.index) > udc.lastFieldIndex {
			udc.lastFieldIndex = int(fieldRow.index)
		}
	}
	err = it.GetError()
	if err != nil {
		return
	}

	return
}

func (udc *UpsideDownCouch) batchRows(addRows []UpsideDownCouchRow, updateRows []UpsideDownCouchRow, deleteRows []UpsideDownCouchRow) (err error) {
	ro := defaultReadOptions()

	// prepare batch
	wb := levigo.NewWriteBatch()

	// add
	for _, row := range addRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to increment counter
			tr := NewTermFrequencyRow(tfr.term, tfr.field, "", 0, 0)
			val, err := udc.db.Get(ro, tr.Key())
			if err != nil {
				return err
			}
			if val != nil {
				tr = ParseFromKeyValue(tr.Key(), val).(*TermFrequencyRow)
				tr.freq += 1 // incr
			} else {
				tr = NewTermFrequencyRow(tfr.term, tfr.field, "", 1, 0)
			}

			// now add this to the batch
			wb.Put(tr.Key(), tr.Value())
		}
		wb.Put(row.Key(), row.Value())
	}

	// update
	for _, row := range updateRows {
		wb.Put(row.Key(), row.Value())
	}

	// delete
	for _, row := range deleteRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to decrement counter
			tr := NewTermFrequencyRow(tfr.term, tfr.field, "", 0, 0)
			val, err := udc.db.Get(ro, tr.Key())
			if err != nil {
				return err
			}
			if val != nil {
				tr = ParseFromKeyValue(tr.Key(), val).(*TermFrequencyRow)
				tr.freq -= 1 // incr
			} else {
				log.Panic(fmt.Sprintf("unexpected missing row, deleting term, expected count row to exit: %v", tr.Key()))
			}

			if tr.freq == 0 {
				wb.Delete(tr.Key())
			} else {
				// now add this to the batch
				wb.Put(tr.Key(), tr.Value())
			}

		}
		wb.Delete(row.Key())
	}

	// write out the batch
	wo := defaultWriteOptions()
	err = udc.db.Write(wo, wb)
	return
}

func (udc *UpsideDownCouch) DocCount() uint64 {
	return udc.docCount
}

func (udc *UpsideDownCouch) Open() (err error) {
	udc.db, err = levigo.Open(udc.path, udc.opts)
	if err != nil {
		return
	}

	ro := defaultReadOptions()
	var value []byte
	value, err = udc.db.Get(ro, VERSION_KEY)
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
	ro := defaultReadOptions()
	ro.SetFillCache(false) // dont fill the cache with this
	it := udc.db.NewIterator(ro)
	defer it.Close()

	// begining of back index
	it.Seek([]byte{'b'})

	var rv uint64 = 0
	for it = it; it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), []byte{'b'}) {
			break
		}
		rv += 1
	}
	return rv
}

func (udc *UpsideDownCouch) rowCount() uint64 {
	ro := defaultReadOptions()
	ro.SetFillCache(false) // dont fill the cache with this
	it := udc.db.NewIterator(ro)
	defer it.Close()

	it.Seek([]byte{0})

	var rv uint64 = 0
	for it = it; it.Valid(); it.Next() {
		rv += 1
	}
	return rv
}

func (udc *UpsideDownCouch) Close() {
	udc.db.Close()
}

func (udc *UpsideDownCouch) Update(doc *document.Document) error {
	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	backIndexRow, err := udc.backIndexRowForDoc(doc.ID)
	if err != nil {
		return err
	}

	var isAdd = true
	// a map for each field, map key is term (string) bool true for existence
	// FIMXE hard-coded to max of 256 fields
	existingTermFieldMaps := make([]map[string]bool, 256)
	if backIndexRow != nil {
		isAdd = false
		for _, entry := range backIndexRow.entries {
			existingTermFieldMap := existingTermFieldMaps[entry.field]
			if existingTermFieldMap == nil {
				existingTermFieldMap = make(map[string]bool, 0)
				existingTermFieldMaps[entry.field] = existingTermFieldMap
			}
			existingTermFieldMap[string(entry.term)] = true
		}
	}

	// prepare a list of rows
	updateRows := make([]UpsideDownCouchRow, 0)
	addRows := make([]UpsideDownCouchRow, 0)

	// track our back index entries
	backIndexEntries := make([]*BackIndexEntry, 0)

	for _, field := range doc.Fields {
		fieldIndex, fieldExists := udc.fieldIndexes[field.Name]
		if !fieldExists {
			// assign next field id
			fieldIndex = uint16(udc.lastFieldIndex + 1)
			udc.fieldIndexes[field.Name] = fieldIndex
			// ensure this batch adds a row for this field
			row := NewFieldRow(uint16(fieldIndex), field.Name)
			updateRows = append(updateRows, row)
			udc.lastFieldIndex = int(fieldIndex)
		}

		existingTermFieldMap := existingTermFieldMaps[fieldIndex]

		analyzer := field.Analyzer
		tokens := analyzer.Analyze(field.Value)
		fieldLength := len(tokens) // number of tokens in this doc field
		fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))
		tokenFreqs := analysis.TokenFrequency(tokens)
		for _, tf := range tokenFreqs {
			var termFreqRow *TermFrequencyRow
			if document.IncludeTermVectors(field.IndexingOptions) {
				tv := termVectorsFromTokenFreq(uint16(fieldIndex), tf)
				termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, uint16(fieldIndex), doc.ID, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
			} else {
				termFreqRow = NewTermFrequencyRow(tf.Term, uint16(fieldIndex), doc.ID, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
			}

			// record the back index entry
			backIndexEntry := BackIndexEntry{tf.Term, uint16(fieldIndex)}
			backIndexEntries = append(backIndexEntries, &backIndexEntry)

			// remove the entry from the map of existing term fields if it exists
			if existingTermFieldMap != nil {
				termString := string(tf.Term)
				_, ok := existingTermFieldMap[termString]
				if ok {
					// this is an update
					updateRows = append(updateRows, termFreqRow)
					// this term existed last time, delete it from that map
					delete(existingTermFieldMap, termString)
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

	// build the back index row
	backIndexRow = NewBackIndexRow(doc.ID, backIndexEntries)
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

	// also delete the back entry itself
	rows = append(rows, backIndexRow)

	err = udc.batchRows(nil, nil, rows)
	if err == nil {
		udc.docCount -= 1
	}
	return err
}

func (udc *UpsideDownCouch) backIndexRowForDoc(docId string) (*BackIndexRow, error) {
	ro := defaultReadOptions()

	// use a temporary row structure to build key
	tempRow := &BackIndexRow{
		doc: []byte(docId),
	}
	key := tempRow.Key()
	value, err := udc.db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	backIndexRow := ParseFromKeyValue(key, value).(*BackIndexRow)
	return backIndexRow, nil
}

func (udc *UpsideDownCouch) Dump() {
	ro := defaultReadOptions()
	ro.SetFillCache(false)
	it := udc.db.NewIterator(ro)
	defer it.Close()
	it.SeekToFirst()
	for it = it; it.Valid(); it.Next() {
		//fmt.Printf("Key: `%v`               Value: `%v`\n", string(it.Key()), string(it.Value()))
		row := ParseFromKeyValue(it.Key(), it.Value())
		if row != nil {
			fmt.Printf("%v\n", row)
			fmt.Printf("Key:   % -100x\nValue: % -100x\n\n", it.Key(), it.Value())
		}
	}
	err := it.GetError()
	if err != nil {
		fmt.Printf("Error reading iterator: %v", err)
	}
}

func (udc *UpsideDownCouch) TermFieldReader(term []byte, fieldName string) (index.TermFieldReader, error) {
	fieldIndex, fieldExists := udc.fieldIndexes[fieldName]
	if fieldExists {
		return newUpsideDownCouchTermFieldReader(udc, term, uint16(fieldIndex))
	}
	log.Printf("fields: %v", udc.fieldIndexes)
	return nil, fmt.Errorf("No field named `%s` in the schema", fieldName)
}

func defaultWriteOptions() *levigo.WriteOptions {
	wo := levigo.NewWriteOptions()
	// request fsync on write for safety
	wo.SetSync(true)
	return wo
}

func defaultReadOptions() *levigo.ReadOptions {
	ro := levigo.NewReadOptions()
	return ro
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
