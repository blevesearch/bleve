//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//go:generate protoc --gofast_out=. smolder.proto

package smolder

import (
	"encoding/binary"
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
	"github.com/seiflotfy/cuckoofilter"

	"github.com/golang/protobuf/proto"
)

const Name = "smolder"

// RowBufferSize should ideally this is sized to be the smallest
// size that can contain an index row key and its corresponding
// value.  It is not a limit, if need be a larger buffer is
// allocated, but performance will be more optimal if *most*
// rows fit this size.
const RowBufferSize = 4 * 1024

var VersionKey = []byte{'v'}

const Version uint8 = 6

var IncompatibleVersion = fmt.Errorf("incompatible version, %d is supported", Version)

type SmolderingCouch struct {
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

	maxInternalDocID uint64

	cf *cuckoofilter.CuckooFilter
}

type docBackIndexRow struct {
	docID        index.IndexInternalID
	doc          *document.Document // If deletion, doc will be nil.
	backIndexRow *BackIndexRow
}

func NewSmolderingCouch(storeName string, storeConfig map[string]interface{}, analysisQueue *index.AnalysisQueue) (index.Index, error) {
	rv := &SmolderingCouch{
		version:       Version,
		fieldCache:    index.NewFieldCache(),
		storeName:     storeName,
		storeConfig:   storeConfig,
		analysisQueue: analysisQueue,
	}
	rv.stats = &indexStat{i: rv}
	return rv, nil
}

func (udc *SmolderingCouch) init(kvwriter store.KVWriter) (err error) {
	// version marker
	rowsAll := [][]SmolderingCouchRow{
		{NewVersionRow(udc.version)},
		{NewFieldRow(0, "_id")},
	}

	udc.fieldCache.AddExisting("_id", 0)

	err = udc.batchRows(kvwriter, nil, rowsAll, nil)
	return
}

func (udc *SmolderingCouch) loadSchema(kvreader store.KVReader) (err error) {

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

func (udc *SmolderingCouch) batchRows(writer store.KVWriter, addRowsAll [][]SmolderingCouchRow, updateRowsAll [][]SmolderingCouchRow, deleteRowsAll [][]SmolderingCouchRow) (err error) {
	dictionaryDeltas := make(map[string]int64)

	// count up bytes needed for buffering.
	addNum := 0
	addKeyBytes := 0
	addValBytes := 0

	updateNum := 0
	updateKeyBytes := 0
	updateValBytes := 0

	deleteNum := 0
	deleteKeyBytes := 0

	rowBuf := GetRowBuffer()

	for _, addRows := range addRowsAll {
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
				dictionaryDeltas[string(rowBuf[:dictKeySize])] += 1
			}
			addKeyBytes += row.KeySize()
			addValBytes += row.ValueSize()
		}
		addNum += len(addRows)
	}

	for _, updateRows := range updateRowsAll {
		for _, row := range updateRows {
			updateKeyBytes += row.KeySize()
			updateValBytes += row.ValueSize()
		}
		updateNum += len(updateRows)
	}

	for _, deleteRows := range deleteRowsAll {
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
				dictionaryDeltas[string(rowBuf[:dictKeySize])] -= 1
			}
			deleteKeyBytes += row.KeySize()
		}
		deleteNum += len(deleteRows)
	}

	PutRowBuffer(rowBuf)

	mergeNum := len(dictionaryDeltas)
	mergeKeyBytes := 0
	mergeValBytes := mergeNum * DictionaryRowMaxValueSize

	for dictRowKey := range dictionaryDeltas {
		mergeKeyBytes += len(dictRowKey)
	}

	// prepare batch
	totBytes := addKeyBytes + addValBytes +
		updateKeyBytes + updateValBytes +
		deleteKeyBytes +
		2*(mergeKeyBytes+mergeValBytes)

	buf, wb, err := writer.NewBatchEx(store.KVBatchOptions{
		TotalBytes: totBytes,
		NumSets:    addNum + updateNum,
		NumDeletes: deleteNum,
		NumMerges:  mergeNum,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = wb.Close()
	}()

	// fill the batch
	for _, addRows := range addRowsAll {
		for _, row := range addRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			valSize, err := row.ValueTo(buf[keySize:])
			if err != nil {
				return err
			}
			wb.Set(buf[:keySize], buf[keySize:keySize+valSize])
			buf = buf[keySize+valSize:]
		}
	}

	for _, updateRows := range updateRowsAll {
		for _, row := range updateRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			valSize, err := row.ValueTo(buf[keySize:])
			if err != nil {
				return err
			}
			wb.Set(buf[:keySize], buf[keySize:keySize+valSize])
			buf = buf[keySize+valSize:]
		}
	}

	for _, deleteRows := range deleteRowsAll {
		for _, row := range deleteRows {
			keySize, err := row.KeyTo(buf)
			if err != nil {
				return err
			}
			wb.Delete(buf[:keySize])
			buf = buf[keySize:]
		}
	}

	for dictRowKey, delta := range dictionaryDeltas {
		dictRowKeyLen := copy(buf, dictRowKey)
		binary.LittleEndian.PutUint64(buf[dictRowKeyLen:], uint64(delta))
		wb.Merge(buf[:dictRowKeyLen], buf[dictRowKeyLen:dictRowKeyLen+DictionaryRowMaxValueSize])
		buf = buf[dictRowKeyLen+DictionaryRowMaxValueSize:]
	}

	// write out the batch
	return writer.ExecuteBatch(wb)
}

func (udc *SmolderingCouch) Open() (err error) {
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

	udc.cf = cuckoofilter.NewDefaultCuckooFilter()

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
		udc.docCount, udc.maxInternalDocID, err = udc.countDocs(kvreader)
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

func (udc *SmolderingCouch) countDocs(kvreader store.KVReader) (count, highDocNum uint64, err error) {
	k := TermFrequencyRowStartField(0)
	it := kvreader.PrefixIterator(k)
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	var lastValidDocNum []byte
	k, _, valid := it.Current()
	for valid {
		tfr, err := NewTermFrequencyRowK(k)
		if err != nil {
			return 0, 0, err
		}
		if tfr.term != nil {
			udc.cf.Insert(tfr.term)
		}
		lastValidDocNum = lastValidDocNum[:0]
		lastValidDocNum = append(lastValidDocNum, tfr.docNumber...)
		count++
		it.Next()
		k, _, valid = it.Current()
	}

	if lastValidDocNum != nil {
		_, highDocNum, err = DecodeUvarintAscending(lastValidDocNum)
		if err != nil {
			return 0, 0, err
		}
	}

	return
}

func (udc *SmolderingCouch) rowCount() (count uint64, err error) {
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

func (udc *SmolderingCouch) Close() error {
	return udc.store.Close()
}

func (udc *SmolderingCouch) Update(doc *document.Document) (err error) {

	// get the next available doc number
	doc.Number = atomic.AddUint64(&udc.maxInternalDocID, 1)

	analysisStart := time.Now()
	numPlainTextBytes := doc.NumPlainTextBytes()
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

	indexReader, err := udc.reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	if udc.cf.Lookup([]byte(doc.ID)) {
		backIndexRow, err = indexReader.backIndexRowForDoc(nil, doc.ID)
		if err != nil {
			_ = indexReader.Close()
			atomic.AddUint64(&udc.stats.errors, 1)
			return
		}
	}

	err = indexReader.Close()
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
	var addRowsAll [][]SmolderingCouchRow
	var updateRowsAll [][]SmolderingCouchRow
	var deleteRowsAll [][]SmolderingCouchRow

	addRows, updateRows, deleteRows := udc.mergeOldAndNew(doc.ID, backIndexRow, result.Rows)
	if len(addRows) > 0 {
		addRowsAll = append(addRowsAll, addRows)
	}
	if len(updateRows) > 0 {
		updateRowsAll = append(updateRowsAll, updateRows)
	}
	if len(deleteRows) > 0 {
		deleteRowsAll = append(deleteRowsAll, deleteRows)
	}

	err = udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll)
	if err == nil && backIndexRow == nil {
		udc.m.Lock()
		udc.docCount++
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		udc.cf.Insert([]byte(doc.ID))
		atomic.AddUint64(&udc.stats.updates, 1)
		atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *SmolderingCouch) mergeOldAndNew(externalDocId string, backIndexRow *BackIndexRow, rows []index.IndexRow) (addRows []SmolderingCouchRow, updateRows []SmolderingCouchRow, deleteRows []SmolderingCouchRow) {
	addRows = make([]SmolderingCouchRow, 0, len(rows))
	updateRows = make([]SmolderingCouchRow, 0, len(rows))
	deleteRows = make([]SmolderingCouchRow, 0, len(rows))

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
		case *BackIndexRow:
			if backIndexRow != nil {
				row.docNumber = backIndexRow.docNumber
				// look through the backindex and update the term entry for _id
				for _, te := range row.termsEntries {
					if *te.Field == 0 {
						for i := range te.Terms {
							te.Terms[i] = externalDocId
						}
					}
				}
			}
			updateRows = append(updateRows, row)
		case *TermFrequencyRow:
			if backIndexRow != nil {
				// doc number could have changed if this is update
				row.docNumber = backIndexRow.docNumber
			}
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
			if backIndexRow != nil {
				// doc number could have changed if this is update
				row.docNumber = backIndexRow.docNumber
			}
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

func (udc *SmolderingCouch) storeField(docNum []byte, field document.Field, fieldIndex uint16, rows []index.IndexRow, backIndexStoredEntries []*BackIndexStoreEntry) ([]index.IndexRow, []*BackIndexStoreEntry) {
	fieldType := encodeFieldType(field)
	storedRow := NewStoredRow(docNum, fieldIndex, field.ArrayPositions(), fieldType, field.Value())

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
	case *document.BooleanField:
		fieldType = 'b'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}

func (udc *SmolderingCouch) indexField(docNum []byte, includeTermVectors bool, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies, rows []index.IndexRow, backIndexTermsEntries []*BackIndexTermsEntry) ([]index.IndexRow, []*BackIndexTermsEntry) {
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	terms := make([]string, 0, len(tokenFreqs))
	for k, tf := range tokenFreqs {
		var termFreqRow *TermFrequencyRow
		if includeTermVectors {
			var tv []*TermVector
			tv, rows = udc.termVectorsFromTokenFreq(fieldIndex, tf, rows)
			termFreqRow = NewTermFrequencyRowWithTermVectors(tf.Term, fieldIndex, docNum, uint64(frequencyFromTokenFreq(tf)), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFrequencyRow(tf.Term, fieldIndex, docNum, uint64(frequencyFromTokenFreq(tf)), fieldNorm)
		}

		// record the back index entry
		terms = append(terms, k)
		rows = append(rows, termFreqRow)
	}

	backIndexTermsEntry := BackIndexTermsEntry{Field: proto.Uint32(uint32(fieldIndex)), Terms: terms}
	backIndexTermsEntries = append(backIndexTermsEntries, &backIndexTermsEntry)

	return rows, backIndexTermsEntries
}

func (udc *SmolderingCouch) Delete(id string) (err error) {
	indexStart := time.Now()

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	indexReader, err := udc.reader()
	if err != nil {
		return
	}

	// first we lookup the backindex row for the doc id if it exists
	// lookup the back index row
	var backIndexRow *BackIndexRow
	backIndexRow, err = indexReader.backIndexRowForDoc(nil, id)
	if err != nil {
		_ = indexReader.Close()
		atomic.AddUint64(&udc.stats.errors, 1)
		return
	}

	err = indexReader.Close()
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

	var deleteRowsAll [][]SmolderingCouchRow

	deleteRows := udc.deleteSingle(backIndexRow.docNumber, backIndexRow, nil)
	if len(deleteRows) > 0 {
		deleteRowsAll = append(deleteRowsAll, deleteRows)
	}

	err = udc.batchRows(kvwriter, nil, nil, deleteRowsAll)
	if err == nil {
		udc.m.Lock()
		udc.docCount--
		udc.m.Unlock()
	}
	atomic.AddUint64(&udc.stats.indexTime, uint64(time.Since(indexStart)))
	if err == nil {
		udc.cf.Delete([]byte(id))
		atomic.AddUint64(&udc.stats.deletes, 1)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *SmolderingCouch) deleteSingle(id index.IndexInternalID, backIndexRow *BackIndexRow, deleteRows []SmolderingCouchRow) []SmolderingCouchRow {
	for _, backIndexEntry := range backIndexRow.termsEntries {
		for i := range backIndexEntry.Terms {
			tfr := TermFrequencyRowDocNumBytes([]byte(backIndexEntry.Terms[i]), uint16(*backIndexEntry.Field), id)
			deleteRows = append(deleteRows, tfr)
		}
	}
	for _, se := range backIndexRow.storedEntries {
		sf := NewStoredRowDocBytes(id, uint16(*se.Field), se.ArrayPositions, 'x', nil)
		deleteRows = append(deleteRows, sf)
	}

	// also delete the back entry itself
	deleteRows = append(deleteRows, backIndexRow)
	return deleteRows
}

func decodeFieldType(typ byte, name string, pos []uint64, value []byte) document.Field {
	switch typ {
	case 't':
		return document.NewTextField(name, pos, value)
	case 'n':
		return document.NewNumericFieldFromBytes(name, pos, value)
	case 'd':
		return document.NewDateTimeFieldFromBytes(name, pos, value)
	case 'b':
		return document.NewBooleanFieldFromBytes(name, pos, value)
	}
	return nil
}

func frequencyFromTokenFreq(tf *analysis.TokenFreq) int {
	return tf.Frequency()
}

func (udc *SmolderingCouch) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq, rows []index.IndexRow) ([]*TermVector, []index.IndexRow) {
	rv := make([]*TermVector, len(tf.Locations))

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = udc.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				rows = append(rows, newFieldRow)
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

	return rv, rows
}

func (udc *SmolderingCouch) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	if len(in) <= 0 {
		return nil
	}

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

func (udc *SmolderingCouch) Batch(batch *index.Batch) (err error) {
	// acquire enough doc numbers for all updates in the batch
	// FIXME we actually waste doc numbers because deletes are in the
	// same map and we don't need numbers for them
	lastDocNumber := atomic.AddUint64(&udc.maxInternalDocID, uint64(len(batch.IndexOps)))
	nextDocNumber := lastDocNumber - uint64(len(batch.IndexOps)) + 1

	analysisStart := time.Now()

	resultChan := make(chan *index.AnalysisResult, len(batch.IndexOps))

	var numUpdates uint64
	var numPlainTextBytes uint64
	for _, doc := range batch.IndexOps {
		if doc != nil {
			doc.Number = nextDocNumber // actually assign doc numbers here
			nextDocNumber++
			numUpdates++
			numPlainTextBytes += doc.NumPlainTextBytes()
		}
	}

	go func() {
		for _, doc := range batch.IndexOps {
			if doc != nil {
				aw := index.NewAnalysisWork(udc, doc, resultChan)
				// put the work on the queue
				udc.analysisQueue.Queue(aw)
			}
		}
	}()

	// retrieve back index rows concurrent with analysis
	docBackIndexRowErr := error(nil)
	docBackIndexRowCh := make(chan *docBackIndexRow, len(batch.IndexOps))

	udc.writeMutex.Lock()
	defer udc.writeMutex.Unlock()

	go func() {
		defer close(docBackIndexRowCh)

		// open a reader for backindex lookup

		indexReader, err := udc.reader()
		if err != nil {
			docBackIndexRowErr = err
			return
		}

		for docID, doc := range batch.IndexOps {
			var backIndexRow *BackIndexRow
			if udc.cf.Lookup([]byte(docID)) {
				backIndexRow, err = indexReader.backIndexRowForDoc(nil, docID)
				if err != nil {
					docBackIndexRowErr = err
					return
				}
			}

			var docNumber []byte
			if backIndexRow != nil {
				docNumber = backIndexRow.docNumber
			}
			docBackIndexRowCh <- &docBackIndexRow{docNumber, doc, backIndexRow}
		}

		err = indexReader.Close()
		if err != nil {
			docBackIndexRowErr = err
			return
		}
	}()

	// wait for analysis result
	newRowsMap := make(map[string][]index.IndexRow)
	var itemsDeQueued uint64
	for itemsDeQueued < numUpdates {
		result := <-resultChan
		newRowsMap[result.DocID] = result.Rows
		itemsDeQueued++
	}
	close(resultChan)

	atomic.AddUint64(&udc.stats.analysisTime, uint64(time.Since(analysisStart)))

	docsAdded := uint64(0)
	docsDeleted := uint64(0)

	indexStart := time.Now()

	// prepare a list of rows
	var addRowsAll [][]SmolderingCouchRow
	var updateRowsAll [][]SmolderingCouchRow
	var deleteRowsAll [][]SmolderingCouchRow

	// add the internal ops
	var updateRows []SmolderingCouchRow
	var deleteRows []SmolderingCouchRow

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

	if len(updateRows) > 0 {
		updateRowsAll = append(updateRowsAll, updateRows)
	}
	if len(deleteRows) > 0 {
		deleteRowsAll = append(deleteRowsAll, deleteRows)
	}

	// process back index rows as they arrive
	for dbir := range docBackIndexRowCh {
		if dbir.doc == nil && dbir.backIndexRow != nil {
			// delete
			deleteRows := udc.deleteSingle(dbir.docID, dbir.backIndexRow, nil)
			if len(deleteRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, deleteRows)
			}
			docsDeleted++
		} else if dbir.doc != nil {
			addRows, updateRows, deleteRows := udc.mergeOldAndNew(dbir.doc.ID, dbir.backIndexRow, newRowsMap[dbir.doc.ID])
			if len(addRows) > 0 {
				addRowsAll = append(addRowsAll, addRows)
			}
			if len(updateRows) > 0 {
				updateRowsAll = append(updateRowsAll, updateRows)
			}
			if len(deleteRows) > 0 {
				deleteRowsAll = append(deleteRowsAll, deleteRows)
			}
			if dbir.backIndexRow == nil {
				docsAdded++
			}
		}
	}

	if docBackIndexRowErr != nil {
		return docBackIndexRowErr
	}

	// start a writer for this batch
	var kvwriter store.KVWriter
	kvwriter, err = udc.store.Writer()
	if err != nil {
		return
	}

	err = udc.batchRows(kvwriter, addRowsAll, updateRowsAll, deleteRowsAll)
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
		for did, doc := range batch.IndexOps {
			if doc != nil {
				udc.cf.Insert([]byte(did))
			} else {
				udc.cf.Delete([]byte(did))
			}
		}
		atomic.AddUint64(&udc.stats.updates, numUpdates)
		atomic.AddUint64(&udc.stats.deletes, docsDeleted)
		atomic.AddUint64(&udc.stats.batches, 1)
		atomic.AddUint64(&udc.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	} else {
		atomic.AddUint64(&udc.stats.errors, 1)
	}
	return
}

func (udc *SmolderingCouch) SetInternal(key, val []byte) (err error) {
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

func (udc *SmolderingCouch) DeleteInternal(key []byte) (err error) {
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

func (udc *SmolderingCouch) Reader() (index.IndexReader, error) {
	return udc.reader()
}

func (udc *SmolderingCouch) reader() (*IndexReader, error) {
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

func (udc *SmolderingCouch) Stats() json.Marshaler {
	return udc.stats
}

func (udc *SmolderingCouch) StatsMap() map[string]interface{} {
	return udc.stats.statsMap()
}

func (udc *SmolderingCouch) Advanced() (store.KVStore, error) {
	return udc.store, nil
}

func (udc *SmolderingCouch) fieldIndexOrNewRow(name string) (uint16, *FieldRow) {
	index, existed := udc.fieldCache.FieldNamed(name, true)
	if !existed {
		return index, NewFieldRow(index, name)
	}
	return index, nil
}

func init() {
	registry.RegisterIndexType(Name, NewSmolderingCouch)
}
