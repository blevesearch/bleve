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
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"

	"github.com/golang/protobuf/proto"
)

var VersionKey = []byte{'v'}

var UnsafeBatchUseDetected = fmt.Errorf("bleve.Batch is NOT thread-safe, modification after execution detected")

const Version uint8 = 5

var IncompatibleVersion = fmt.Errorf("incompatible version, %d is supported", Version)

type UpsideDownCouch struct {
	version         uint8
	path            string
	store           store.KVStore
	fieldIndexCache *FieldIndexCache
	analysisQueue   *AnalysisQueue
	stats           *indexStat

	m sync.RWMutex
	// fields protected by m
	docCount uint64
}

func NewUpsideDownCouch(s store.KVStore, analysisQueue *AnalysisQueue) *UpsideDownCouch {
	return &UpsideDownCouch{
		version:         Version,
		fieldIndexCache: NewFieldIndexCache(),
		store:           s,
		analysisQueue:   analysisQueue,
		stats:           &indexStat{},
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
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	it.Seek(keyPrefix)
	key, val, valid := it.Current()
	for valid {

		// stop when
		if !bytes.HasPrefix(key, keyPrefix) {
			break
		}
		var fieldRow *FieldRow
		fieldRow, err = NewFieldRowKV(key, val)
		if err != nil {
			return
		}
		udc.fieldIndexCache.AddExisting(fieldRow.name, fieldRow.index)

		it.Next()
		key, val, valid = it.Current()
	}

	keyPrefix = []byte{'v'}
	val, err = kvreader.Get(keyPrefix)
	if err != nil {
		return
	}
	var vr *VersionRow
	vr, err = NewVersionRowKV(keyPrefix, val)
	if err != nil {
		return
	}
	if vr.version != Version {
		err = IncompatibleVersion
		return
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
			dictionaryKey := tfr.DictionaryRowKey()
			wb.Merge(dictionaryKey, dictionaryTermIncr)
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
			dictionaryKey := tfr.DictionaryRowKey()
			wb.Merge(dictionaryKey, dictionaryTermDecr)
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

func (udc *UpsideDownCouch) DocCount() (uint64, error) {
	udc.m.RLock()
	defer udc.m.RUnlock()
	return udc.docCount, nil
}

func (udc *UpsideDownCouch) Open() (err error) {
	// install the merge operator
	udc.store.SetMergeOperator(&mergeOperator)

	// now open the kv store
	err = udc.store.Open()
	if err != nil {
		return
	}

	// start a writer for the open process
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	var value []byte
	value, err = kvwriter.Get(VersionKey)
	if err != nil {
		return
	}

	// init new index OR load schema
	if value == nil {
		err = udc.init(kvwriter)
		if err != nil {
			return
		}
	} else {
		err = udc.loadSchema(kvwriter)
		if err != nil {
			return
		}
	}
	// set doc count
	udc.m.Lock()
	udc.docCount, err = udc.countDocs(kvwriter)
	udc.m.Unlock()
	return
}

func (udc *UpsideDownCouch) countDocs(kvreader store.KVReader) (count uint64, err error) {
	it := kvreader.Iterator([]byte{'b'})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	key, _, valid := it.Current()
	for valid {
		if !bytes.HasPrefix(key, []byte{'b'}) {
			break
		}
		count++
		it.Next()
		key, _, valid = it.Current()
	}

	return
}

func (udc *UpsideDownCouch) rowCount() (count uint64, err error) {
	// start an isolated reader for use during the rowcount
	kvreader, err := udc.store.Reader()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvreader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	it := kvreader.Iterator([]byte{0})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	_, _, valid := it.Current()
	for valid {
		count++
		it.Next()
		_, _, valid = it.Current()
	}

	return
}

func (udc *UpsideDownCouch) Close() error {
	return udc.store.Close()
}

func (udc *UpsideDownCouch) Update(doc *document.Document) (err error) {
	// do analysis before acquiring write lock
	analysisStart := time.Now()
	resultChan := make(chan *AnalysisResult)
	aw := AnalysisWork{
		udc: udc,
		d:   doc,
		rc:  resultChan,
	}
	// put the work on the queue
	go func() {
		udc.analysisQueue.Queue(&aw)
	}()

	// wait for the result
	result := <-resultChan
	close(resultChan)
	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	// start a writer for this update
	indexStart := time.Now()
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = udc.backIndexRowForDoc(kvwriter, doc.ID)
	if err != nil {
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	addRows, updateRows, deleteRows = udc.mergeOldAndNew(backIndexRow, result.rows, addRows, updateRows, deleteRows)

	err = udc.batchRows(kvwriter, addRows, updateRows, deleteRows)
	if err == nil && backIndexRow == nil {
		udc.m.Lock()
		udc.docCount++
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		atomic.AddUint64(&udc.stats.updates, 1)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *UpsideDownCouch) mergeOldAndNew(backIndexRow *BackIndexRow, rows, addRows, updateRows, deleteRows []UpsideDownCouchRow) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []UpsideDownCouchRow) {
	existingTermKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllTermKeys() {
		existingTermKeys[string(key)] = true
	}

	existingStoredKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllStoredKeys() {
		existingStoredKeys[string(key)] = true
	}

	for _, row := range rows {
		switch row := row.(type) {
		case *TermFrequencyRow:
			rowKey := string(row.Key())
			if _, ok := existingTermKeys[rowKey]; ok {
				updateRows = append(updateRows, row)
				delete(existingTermKeys, rowKey)
			} else {
				addRows = append(addRows, row)
			}
		case *StoredRow:
			rowKey := string(row.Key())
			if _, ok := existingStoredKeys[rowKey]; ok {
				updateRows = append(updateRows, row)
				delete(existingStoredKeys, rowKey)
			} else {
				addRows = append(addRows, row)
			}
		default:
			updateRows = append(updateRows, row)
		}

	}

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

func (udc *UpsideDownCouch) storeField(docID string, field document.Field, fieldIndex uint16) ([]UpsideDownCouchRow, []*BackIndexStoreEntry) {
	rows := make([]UpsideDownCouchRow, 0, 100)
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docID, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

	// record the back index entry
	backIndexStoredEntry := BackIndexStoreEntry{Field: proto.Uint32(uint32(fieldIndex)), ArrayPositions: field.ArrayPositions()}
	backIndexStoredEntries = append(backIndexStoredEntries, &backIndexStoredEntry)

	rows = append(rows, storedRow)
	return rows, backIndexStoredEntries
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

func (udc *UpsideDownCouch) indexField(docID string, field document.Field, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies) ([]UpsideDownCouchRow, []*BackIndexTermEntry) {

	rows := make([]UpsideDownCouchRow, 0, 100)
	backIndexTermEntries := make([]*BackIndexTermEntry, 0)
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for _, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if field.Options().IncludeTermVectors() {
			tv, newFieldRows := udc.termVectorsFromTokenFreq(fieldIndex, field.ArrayPositions(), tf)
			rows = append(rows, newFieldRows...)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		backIndexTermEntry := BackIndexTermEntry{Term: proto.String(string(tf.Term)), Field: proto.Uint32(uint32(fieldIndex))}
		backIndexTermEntries = append(backIndexTermEntries, &backIndexTermEntry)

		rows = append(rows, termFreqRow)
	}

	return rows, backIndexTermEntries
}

func (udc *UpsideDownCouch) Delete(id string) (err error) {
	indexStart := time.Now()
	// start a writer for this delete
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = udc.backIndexRowForDoc(kvwriter, id)
	if err != nil {
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}
	if backIndexRow == nil {
		atomic.AddUint64(&udc.stats.deletes, 1)
		return
	}

	deleteRows := make([]UpsideDownCouchRow, 0)
	deleteRows = udc.deleteSingle(id, backIndexRow, deleteRows)

	err = udc.batchRows(kvwriter, nil, nil, deleteRows)
	if err == nil {
		udc.m.Lock()
		udc.docCount--
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		atomic.AddUint64(&udc.stats.deletes, 1)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
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

func (udc *UpsideDownCouch) backIndexRowsForBatch(kvreader store.KVReader, batch *index.Batch) (map[string]*BackIndexRow, error) {
	// FIXME faster to order the ids and scan sequentially
	// for now just get it working
	rv := make(map[string]*BackIndexRow, 0)
	for docID := range batch.IndexOps {
		backIndexRow, err := udc.backIndexRowForDoc(kvreader, docID)
		if err != nil {
			return nil, err
		}
		rv[docID] = backIndexRow
	}
	return rv, nil
}

func decodeFieldType(typ byte, name string, pos []uint64, value []byte) document.Field {
	switch typ {
	case 't':
		return document.NewTextField(name, pos, value)
	case 'n':
		return document.NewNumericFieldFromBytes(name, pos, value)
	case 'd':
		return document.NewDateTimeFieldFromBytes(name, pos, value)
	}
	return nil
}

func frequencyFromTokenFreq(tf *analysis.TokenFreq) int {
	return len(tf.Locations)
}

func (udc *UpsideDownCouch) termVectorsFromTokenFreq(field uint16, arrayPositions []uint64, tf *analysis.TokenFreq) ([]*TermVector, []UpsideDownCouchRow) {
	rv := make([]*TermVector, len(tf.Locations))
	newFieldRows := make([]UpsideDownCouchRow, 0)

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = udc.fieldIndexCache.FieldIndex(l.Field)
			if newFieldRow != nil {
				newFieldRows = append(newFieldRows, newFieldRow)
			}
		}
		tv := TermVector{
			field:          fieldIndex,
			arrayPositions: arrayPositions,
			pos:            uint64(l.Position),
			start:          uint64(l.Start),
			end:            uint64(l.End),
		}
		rv[i] = &tv
	}

	return rv, newFieldRows
}

func (udc *UpsideDownCouch) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	rv := make([]*index.TermFieldVector, len(in))

	for i, tv := range in {
		fieldName := udc.fieldIndexCache.FieldName(tv.field)
		tfv := index.TermFieldVector{
			Field:          fieldName,
			ArrayPositions: tv.arrayPositions,
			Pos:            tv.pos,
			Start:          tv.start,
			End:            tv.end,
		}
		rv[i] = &tfv
	}
	return rv
}

func (udc *UpsideDownCouch) Batch(batch *index.Batch) (err error) {
	analysisStart := time.Now()
	resultChan := make(chan *AnalysisResult)

	var numUpdates uint64
	for _, doc := range batch.IndexOps {
		if doc != nil {
			numUpdates++
		}
	}

	var detectedUnsafeMutex sync.RWMutex
	detectedUnsafe := false

	go func() {
		sofar := uint64(0)
		for _, doc := range batch.IndexOps {
			if doc != nil {
				sofar++
				if sofar > numUpdates {
					detectedUnsafeMutex.Lock()
					detectedUnsafe = true
					detectedUnsafeMutex.Unlock()
					return
				}
				aw := AnalysisWork{
					udc: udc,
					d:   doc,
					rc:  resultChan,
				}
				// put the work on the queue
				udc.analysisQueue.Queue(&aw)
			}
		}
	}()

	newRowsMap := make(map[string][]UpsideDownCouchRow)
	// wait for the result
	var itemsDeQueued uint64
	for itemsDeQueued < numUpdates {
		result := <-resultChan
		newRowsMap[result.docID] = result.rows
		itemsDeQueued++
	}
	close(resultChan)

	detectedUnsafeMutex.RLock()
	defer detectedUnsafeMutex.RUnlock()
	if detectedUnsafe {
		return UnsafeBatchUseDetected
	}

	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	indexStart := time.Now()
	// start a writer for this batch
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	// first lookup all the back index rows
	var backIndexRows map[string]*BackIndexRow
	backIndexRows, err = udc.backIndexRowsForBatch(kvwriter, batch)
	if err != nil {
		return
	}

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	docsAdded := uint64(0)
	docsDeleted := uint64(0)
	for docID, doc := range batch.IndexOps {
		backIndexRow := backIndexRows[docID]
		if doc == nil && backIndexRow != nil {
			// delete
			deleteRows = udc.deleteSingle(docID, backIndexRow, deleteRows)
			docsDeleted++
		} else if doc != nil {
			addRows, updateRows, deleteRows = udc.mergeOldAndNew(backIndexRow, newRowsMap[docID], addRows, updateRows, deleteRows)
			if backIndexRow == nil {
				docsAdded++
			}
		}
	}

	// add the internal ops
	for internalKey, internalValue := range batch.InternalOps {
		if internalValue == nil {
			// delete
			deleteInternalRow := NewInternalRow([]byte(internalKey), nil)
			deleteRows = append(deleteRows, deleteInternalRow)
		} else {
			updateInternalRow := NewInternalRow([]byte(internalKey), internalValue)
			updateRows = append(updateRows, updateInternalRow)
		}
	}

	err = udc.batchRows(kvwriter, addRows, updateRows, deleteRows)
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		udc.m.Lock()
		udc.docCount += docsAdded
		udc.docCount -= docsDeleted
		udc.m.Unlock()
		atomic.AddUint64(&udc.stats.updates, numUpdates)
		atomic.AddUint64(&udc.stats.deletes, docsDeleted)
		atomic.AddUint64(&udc.stats.batches, 1)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *UpsideDownCouch) SetInternal(key, val []byte) (err error) {
	internalRow := NewInternalRow(key, val)
	var writer store.KVWriter
	writer, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	return writer.Set(internalRow.Key(), internalRow.Value())
}

func (udc *UpsideDownCouch) DeleteInternal(key []byte) (err error) {
	internalRow := NewInternalRow(key, nil)
	var writer store.KVWriter
	writer, err = udc.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	return writer.Delete(internalRow.Key())
}

func (udc *UpsideDownCouch) Reader() (index.IndexReader, error) {
	kvr, err := udc.store.Reader()
	if err != nil {
		return nil, fmt.Errorf("error opening store reader: %v", err)
	}
	udc.m.RLock()
	defer udc.m.RUnlock()
	return &IndexReader{
		index:    udc,
		kvreader: kvr,
		docCount: udc.docCount,
	}, nil
}

func (udc *UpsideDownCouch) Stats() json.Marshaler {
	return udc.stats
}
