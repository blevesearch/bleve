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
	"github.com/blevesearch/bleve/registry"

	"github.com/golang/protobuf/proto"
)

const Name = "upside_down"

// RowBufferSize should ideally this is sized to be the smallest
// size that can cotain an index row key and its corresponding
// value.  It is not a limit, if need be a larger buffer is
// allocated, but performance will be more optimal if *most*
// rows fit this size.
const RowBufferSize = 4 * 1024

var VersionKey = []byte{'v'}

var UnsafeBatchUseDetected = fmt.Errorf("bleve.Batch is NOT thread-safe, modification after execution detected")

const Version uint8 = 5

var IncompatibleVersion = fmt.Errorf("incompatible version, %d is supported", Version)

type UpsideDownCouch struct {
	version       uint8
	path          string
	storeName     string
	storeConfig   map[string]interface{}
	store         store.KVStore
	fieldCache    *index.FieldCache
	analysisQueue *index.AnalysisQueue
	stats         *indexStat

	m sync.RWMutex
	// fields protected by m
	docCount uint64

	writeMutex sync.Mutex
}

func NewUpsideDownCouch(storeName string, storeConfig map[string]interface{}, analysisQueue *index.AnalysisQueue) (index.Index, error) {
	return &UpsideDownCouch{
		version:       Version,
		fieldCache:    index.NewFieldCache(),
		storeName:     storeName,
		storeConfig:   storeConfig,
		analysisQueue: analysisQueue,
		stats:         &indexStat{},
	}, nil
}

func (udc *UpsideDownCouch) init(kvwriter store.KVWriter) (err error) {
	// prepare a list of rows
	rows := make([]UpsideDownCouchRow, 0)

	// version marker
	rows = append(rows, NewVersionRow(udc.version))

	err = udc.batchRows(kvwriter, nil, rows, nil)
	return
}

func (udc *UpsideDownCouch) loadSchema(kvreader store.KVReader) (err error) {

	it := kvreader.PrefixIterator([]byte{'f'})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	key, val, valid := it.Current()
	for valid {
		var fieldRow *FieldRow
		fieldRow, err = NewFieldRowKV(key, val)
		if err != nil {
			return
		}
		udc.fieldCache.AddExisting(fieldRow.name, fieldRow.index)

		it.Next()
		key, val, valid = it.Current()
	}

	val, err = kvreader.Get([]byte{'v'})
	if err != nil {
		return
	}
	var vr *VersionRow
	vr, err = NewVersionRowKV([]byte{'v'}, val)
	if err != nil {
		return
	}
	if vr.version != Version {
		err = IncompatibleVersion
		return
	}

	return
}

var rowBufferPool sync.Pool

func GetRowBuffer() []byte {
	if rb, ok := rowBufferPool.Get().([]byte); ok {
		return rb
	} else {
		return make([]byte, RowBufferSize)
	}
}

func PutRowBuffer(buf []byte) {
	rowBufferPool.Put(buf)
}

func (udc *UpsideDownCouch) batchRows(writer store.KVWriter, addRows []UpsideDownCouchRow, updateRows []UpsideDownCouchRow, deleteRows []UpsideDownCouchRow) (err error) {

	// prepare batch
	wb := writer.NewBatch()

	// buffer to work with
	rowBuf := GetRowBuffer()

	// add
	for _, row := range addRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			if tfr.DictionaryRowKeySize() > len(rowBuf) {
				rowBuf = make([]byte, tfr.DictionaryRowKeySize())
			}
			dictKeySize, err := tfr.DictionaryRowKeyTo(rowBuf)
			if err != nil {
				return err
			}
			wb.Merge(rowBuf[:dictKeySize], dictionaryTermIncr)
		}
		if row.KeySize()+row.ValueSize() > len(rowBuf) {
			rowBuf = make([]byte, row.KeySize()+row.ValueSize())
		}
		keySize, err := row.KeyTo(rowBuf)
		if err != nil {
			return err
		}
		valSize, err := row.ValueTo(rowBuf[keySize:])
		wb.Set(rowBuf[:keySize], rowBuf[keySize:keySize+valSize])
	}

	// update
	for _, row := range updateRows {
		if row.KeySize()+row.ValueSize() > len(rowBuf) {
			rowBuf = make([]byte, row.KeySize()+row.ValueSize())
		}
		keySize, err := row.KeyTo(rowBuf)
		if err != nil {
			return err
		}
		valSize, err := row.ValueTo(rowBuf[keySize:])
		if err != nil {
			return err
		}
		wb.Set(rowBuf[:keySize], rowBuf[keySize:keySize+valSize])
	}

	// delete
	for _, row := range deleteRows {
		tfr, ok := row.(*TermFrequencyRow)
		if ok {
			// need to decrement counter
			if tfr.DictionaryRowKeySize() > len(rowBuf) {
				rowBuf = make([]byte, tfr.DictionaryRowKeySize())
			}
			dictKeySize, err := tfr.DictionaryRowKeyTo(rowBuf)
			if err != nil {
				return err
			}
			wb.Merge(rowBuf[:dictKeySize], dictionaryTermDecr)
		}
		if row.KeySize()+row.ValueSize() > len(rowBuf) {
			rowBuf = make([]byte, row.KeySize()+row.ValueSize())
		}
		keySize, err := row.KeyTo(rowBuf)
		if err != nil {
			return err
		}
		wb.Delete(rowBuf[:keySize])
	}

	PutRowBuffer(rowBuf)

	// write out the batch
	return writer.ExecuteBatch(wb)
}

func (udc *UpsideDownCouch) DocCount() (uint64, error) {
	udc.m.RLock()
	defer udc.m.RUnlock()
	return udc.docCount, nil
}

func (udc *UpsideDownCouch) Open() (err error) {
	//acquire the write mutex for the duratin of Open()
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open the kv store
	storeConstructor := registry.KVStoreConstructorByName(udc.storeName)
	if storeConstructor == nil {
		err = index.ErrorUnknownStorageType
		return
	}

	// now open the store
	udc.store, err = storeConstructor(&mergeOperator, udc.storeConfig)
	if err != nil {
		return
	}

	// start a reader to look at the index
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	var value []byte
	value, err = kvreader.Get(VersionKey)
	if err != nil {
		_ = kvreader.Close()
		return
	}

	if value != nil {
		err = udc.loadSchema(kvreader)
		if err != nil {
			_ = kvreader.Close()
			return
		}

		// set doc count
		udc.m.Lock()
		udc.docCount, err = udc.countDocs(kvreader)
		udc.m.Unlock()

		err = kvreader.Close()
	} else {
		// new index, close the reader and open writer to init
		err = kvreader.Close()
		if err != nil {
			return
		}

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

		// init the index
		err = udc.init(kvwriter)
	}

	return
}

func (udc *UpsideDownCouch) countDocs(kvreader store.KVReader) (count uint64, err error) {
	it := kvreader.PrefixIterator([]byte{'b'})
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
	it := kvreader.RangeIterator(nil, nil)
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
	resultChan := make(chan *index.AnalysisResult)
	aw := index.NewAnalysisWork(udc, doc, resultChan)
	// put the work on the queue
	udc.analysisQueue.Queue(aw)

	// wait for the result
	result := <-resultChan
	close(resultChan)
	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open a reader for backindex lookup
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = udc.backIndexRowForDoc(kvreader, doc.ID)
	if err != nil {
		_ = kvreader.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

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

	// prepare a list of rows
	addRows := make([]UpsideDownCouchRow, 0)
	updateRows := make([]UpsideDownCouchRow, 0)
	deleteRows := make([]UpsideDownCouchRow, 0)

	addRows, updateRows, deleteRows = udc.mergeOldAndNew(backIndexRow, result.Rows, addRows, updateRows, deleteRows)

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

func (udc *UpsideDownCouch) mergeOldAndNew(backIndexRow *BackIndexRow, rows []index.IndexRow, addRows, updateRows, deleteRows []UpsideDownCouchRow) ([]UpsideDownCouchRow, []UpsideDownCouchRow, []UpsideDownCouchRow) {
	existingTermKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllTermKeys() {
		existingTermKeys[string(key)] = true
	}

	existingStoredKeys := make(map[string]bool)
	for _, key := range backIndexRow.AllStoredKeys() {
		existingStoredKeys[string(key)] = true
	}

	keyBuf := GetRowBuffer()
	for _, row := range rows {
		switch row := row.(type) {
		case *TermFrequencyRow:
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingTermKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingTermKeys, string(keyBuf[:keySize]))
			} else {
				addRows = append(addRows, row)
			}
		case *StoredRow:
			if row.KeySize() > len(keyBuf) {
				keyBuf = make([]byte, row.KeySize())
			}
			keySize, _ := row.KeyTo(keyBuf)
			if _, ok := existingStoredKeys[string(keyBuf[:keySize])]; ok {
				updateRows = append(updateRows, row)
				delete(existingStoredKeys, string(keyBuf[:keySize]))
			} else {
				addRows = append(addRows, row)
			}
		default:
			updateRows = append(updateRows, row)
		}
	}
	PutRowBuffer(keyBuf)

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

func (udc *UpsideDownCouch) storeField(docID []byte, field document.Field, fieldIndex uint16, rows []index.IndexRow, backIndexStoredEntries []*BackIndexStoreEntry) ([]index.IndexRow, []*BackIndexStoreEntry) {
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docID, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

	// record the back index entry
	backIndexStoredEntry := BackIndexStoreEntry{Field: proto.Uint32(uint32(fieldIndex)), ArrayPositions: field.ArrayPositions()}

	return append(rows, storedRow), append(backIndexStoredEntries, &backIndexStoredEntry)
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

func (udc *UpsideDownCouch) indexField(docID []byte, includeTermVectors bool, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies, rows []index.IndexRow, backIndexTermEntries []*BackIndexTermEntry) ([]index.IndexRow, []*BackIndexTermEntry) {
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for k, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if includeTermVectors {
			tv, newFieldRows := udc.termVectorsFromTokenFreq(fieldIndex, tf)
			rows = append(rows, newFieldRows...)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docID, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		backIndexTermEntry := BackIndexTermEntry{Term: proto.String(k), Field: proto.Uint32(uint32(fieldIndex))}
		backIndexTermEntries = append(backIndexTermEntries, &backIndexTermEntry)

		rows = append(rows, termFreqRow)
	}

	return rows, backIndexTermEntries
}

func (udc *UpsideDownCouch) Delete(id string) (err error) {
	indexStart := time.Now()

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open a reader for backindex lookup
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = udc.backIndexRowForDoc(kvreader, id)
	if err != nil {
		_ = kvreader.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

	if backIndexRow == nil {
		atomic.AddUint64(&udc.stats.deletes, 1)
		return
	}

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
	idBytes := []byte(id)

	for _, backIndexEntry := range backIndexRow.termEntries {
		tfr := NewTermFrequencyRow([]byte(*backIndexEntry.Term), uint16(*backIndexEntry.Field), idBytes, 0, 0)
		deleteRows = append(deleteRows, tfr)
	}
	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRow(idBytes, uint16(*se.Field), se.ArrayPositions, 'x', nil)
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

	keyBuf := GetRowBuffer()
	if tempRow.KeySize() > len(keyBuf) {
		keyBuf = make([]byte, 2*tempRow.KeySize())
	}
	defer PutRowBuffer(keyBuf)
	keySize, err := tempRow.KeyTo(keyBuf)
	if err != nil {
		return nil, err
	}

	value, err := kvreader.Get(keyBuf[:keySize])
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	backIndexRow, err := NewBackIndexRowKV(keyBuf[:keySize], value)
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
	return tf.Frequency()
}

func (udc *UpsideDownCouch) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq) ([]*TermVector, []index.IndexRow) {
	rv := make([]*TermVector, len(tf.Locations))
	newFieldRows := make([]index.IndexRow, 0)

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = udc.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				newFieldRows = append(newFieldRows, newFieldRow)
			}
		}
		tv := TermVector{
			field:          fieldIndex,
			arrayPositions: l.ArrayPositions,
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
		fieldName := udc.fieldCache.FieldIndexed(tv.field)
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
	resultChan := make(chan *index.AnalysisResult)

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
				aw := index.NewAnalysisWork(udc, doc, resultChan)
				// put the work on the queue
				udc.analysisQueue.Queue(aw)
			}
		}
	}()

	newRowsMap := make(map[string][]index.IndexRow)
	// wait for the result
	var itemsDeQueued uint64
	for itemsDeQueued < numUpdates {
		result := <-resultChan
		newRowsMap[result.DocID] = result.Rows
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

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	// open a reader for backindex lookup
	var kvreader store.KVReader
	kvreader, err = udc.store.Reader()
	if err != nil {
		return
	}

	// first lookup all the back index rows
	var backIndexRows map[string]*BackIndexRow
	backIndexRows, err = udc.backIndexRowsForBatch(kvreader, batch)
	if err != nil {
		_ = kvreader.Close()
		return
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

	// start a writer for this batch
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
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
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = kvwriter.Close()
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
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()
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

	batch := writer.NewBatch()
	batch.Set(internalRow.Key(), internalRow.Value())

	return writer.ExecuteBatch(batch)
}

func (udc *UpsideDownCouch) DeleteInternal(key []byte) (err error) {
	internalRow := NewInternalRow(key, nil)
	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()
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

	batch := writer.NewBatch()
	batch.Delete(internalRow.Key())
	return writer.ExecuteBatch(batch)
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

func (udc *UpsideDownCouch) fieldIndexOrNewRow(name string) (uint16, *FieldRow) {
	index, existed := udc.fieldCache.FieldNamed(name, true)
	if !existed {
		return index, NewFieldRow(uint16(index), name)
	}
	return index, nil
}

func init() {
	registry.RegisterIndexType(Name, NewUpsideDownCouch)
}
