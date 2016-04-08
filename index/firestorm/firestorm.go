//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const Name = "firestorm"

type Firestorm struct {
	highDocNumber uint64
	docCount      uint64

	storeName        string
	storeConfig      map[string]interface{}
	store            store.KVStore
	compensator      *Compensator
	analysisQueue    *index.AnalysisQueue
	fieldCache       *index.FieldCache
	garbageCollector *GarbageCollector
	lookuper         *Lookuper
	dictUpdater      *DictUpdater
	stats            *indexStat
}

func NewFirestorm(storeName string, storeConfig map[string]interface{}, analysisQueue *index.AnalysisQueue) (index.Index, error) {
	rv := Firestorm{
		storeName:     storeName,
		storeConfig:   storeConfig,
		compensator:   NewCompensator(),
		analysisQueue: analysisQueue,
		fieldCache:    index.NewFieldCache(),
		docCount:      0,
		highDocNumber: 0,
		stats:         &indexStat{},
	}
	rv.stats.f = &rv
	rv.garbageCollector = NewGarbageCollector(&rv)
	rv.lookuper = NewLookuper(&rv)
	rv.dictUpdater = NewDictUpdater(&rv)
	return &rv, nil
}

func (f *Firestorm) Open() (err error) {

	// open the kv store
	storeConstructor := registry.KVStoreConstructorByName(f.storeName)
	if storeConstructor == nil {
		err = index.ErrorUnknownStorageType
		return
	}

	// now open the store
	f.store, err = storeConstructor(&mergeOperator, f.storeConfig)
	if err != nil {
		return
	}

	// start a reader
	var kvreader store.KVReader
	kvreader, err = f.store.Reader()
	if err != nil {
		return
	}

	// assert correct version, and find out if this is new index
	var newIndex bool
	newIndex, err = f.checkVersion(kvreader)
	if err != nil {
		return
	}

	if !newIndex {
		// process existing index before opening
		err = f.warmup(kvreader)
		if err != nil {
			return
		}
	}

	err = kvreader.Close()
	if err != nil {
		return
	}

	if newIndex {
		// prepare a new index
		err = f.bootstrap()
		if err != nil {
			return
		}
	}

	// start the garbage collector
	f.garbageCollector.Start()

	// start the lookuper
	f.lookuper.Start()

	// start the dict updater
	f.dictUpdater.Start()

	return
}

func (f *Firestorm) Close() error {
	f.garbageCollector.Stop()
	f.lookuper.Stop()
	f.dictUpdater.Stop()
	return f.store.Close()
}

func (f *Firestorm) DocCount() (uint64, error) {
	count := atomic.LoadUint64(&f.docCount)
	return count, nil

}

func (f *Firestorm) Update(doc *document.Document) (err error) {

	// assign this document a number
	doc.Number = atomic.AddUint64(&f.highDocNumber, 1)

	// do analysis before acquiring write lock
	analysisStart := time.Now()
	numPlainTextBytes := doc.NumPlainTextBytes()
	resultChan := make(chan *index.AnalysisResult)
	aw := index.NewAnalysisWork(f, doc, resultChan)

	// put the work on the queue
	f.analysisQueue.Queue(aw)

	// wait for the result
	result := <-resultChan
	close(resultChan)
	atomic.AddUint64(&f.stats.analysisTime, uint64(time.Since(analysisStart)))

	// start a writer for this update
	indexStart := time.Now()
	var kvwriter store.KVWriter
	kvwriter, err = f.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := kvwriter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	var dictionaryDeltas map[string]int64
	dictionaryDeltas, err = f.batchRows(kvwriter, [][]index.IndexRow{result.Rows}, nil)
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&f.stats.errors, 1)
		return
	}

	f.compensator.Mutate([]byte(doc.ID), doc.Number)
	f.lookuper.NotifyBatch([]*InFlightItem{{[]byte(doc.ID), doc.Number}})
	f.dictUpdater.NotifyBatch(dictionaryDeltas)

	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))
	atomic.AddUint64(&f.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	return
}

func (f *Firestorm) Delete(id string) error {
	indexStart := time.Now()
	f.compensator.Mutate([]byte(id), 0)
	f.lookuper.NotifyBatch([]*InFlightItem{{[]byte(id), 0}})
	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))
	return nil
}

func (f *Firestorm) batchRows(writer store.KVWriter, rowsOfRows [][]index.IndexRow, deleteKeys [][]byte) (map[string]int64, error) {

	dictionaryDeltas := make(map[string]int64)

	// count up bytes needed for buffering.
	addNum := 0
	addKeyBytes := 0
	addValBytes := 0

	deleteNum := 0
	deleteKeyBytes := 0

	var kbuf []byte

	prepareBuf := func(buf []byte, sizeNeeded int) []byte {
		if cap(buf) < sizeNeeded {
			return make([]byte, sizeNeeded, sizeNeeded+128)
		}
		return buf[0:sizeNeeded]
	}

	for _, rows := range rowsOfRows {
		for _, row := range rows {
			tfr, ok := row.(*TermFreqRow)
			if ok {
				if tfr.Field() != 0 {
					kbuf = prepareBuf(kbuf, tfr.DictionaryRowKeySize())
					klen, err := tfr.DictionaryRowKeyTo(kbuf)
					if err != nil {
						return nil, err
					}

					dictionaryDeltas[string(kbuf[0:klen])] += 1
				}
			}

			addKeyBytes += row.KeySize()
			addValBytes += row.ValueSize()
		}
		addNum += len(rows)
	}

	for _, dk := range deleteKeys {
		deleteKeyBytes += len(dk)
	}
	deleteNum += len(deleteKeys)

	// prepare batch
	totBytes := addKeyBytes + addValBytes + deleteKeyBytes

	buf, wb, err := writer.NewBatchEx(store.KVBatchOptions{
		TotalBytes: totBytes,
		NumSets:    addNum,
		NumDeletes: deleteNum,
		NumMerges:  0,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = wb.Close()
	}()

	for _, rows := range rowsOfRows {
		for _, row := range rows {
			klen, err := row.KeyTo(buf)
			if err != nil {
				return nil, err
			}

			vlen, err := row.ValueTo(buf[klen:])
			if err != nil {
				return nil, err
			}

			wb.Set(buf[0:klen], buf[klen:klen+vlen])

			buf = buf[klen+vlen:]
		}
	}

	for _, dk := range deleteKeys {
		dklen := copy(buf, dk)
		wb.Delete(buf[0:dklen])
		buf = buf[dklen:]
	}

	// write out the batch
	err = writer.ExecuteBatch(wb)
	if err != nil {
		return nil, err
	}
	return dictionaryDeltas, nil
}

func (f *Firestorm) Batch(batch *index.Batch) (err error) {

	// acquire enough doc numbers for all updates in the batch
	// FIXME we actually waste doc numbers because deletes are in the
	// same map and we don't need numbers for them
	lastDocNumber := atomic.AddUint64(&f.highDocNumber, uint64(len(batch.IndexOps)))
	firstDocNumber := lastDocNumber - uint64(len(batch.IndexOps)) + 1

	analysisStart := time.Now()
	resultChan := make(chan *index.AnalysisResult)

	var docsUpdated uint64
	var docsDeleted uint64
	var numPlainTextBytes uint64
	for _, doc := range batch.IndexOps {
		if doc != nil {
			doc.Number = firstDocNumber // actually assign doc numbers here
			firstDocNumber++
			docsUpdated++
			numPlainTextBytes += doc.NumPlainTextBytes()
		} else {
			docsDeleted++
		}
	}

	go func() {
		for _, doc := range batch.IndexOps {
			if doc != nil {
				aw := index.NewAnalysisWork(f, doc, resultChan)
				// put the work on the queue
				f.analysisQueue.Queue(aw)
			}
		}
	}()

	// extra 1 capacity for internal updates.
	collectRows := make([][]index.IndexRow, 0, docsUpdated+1)

	// wait for the result
	var itemsDeQueued uint64
	for itemsDeQueued < docsUpdated {
		result := <-resultChan
		collectRows = append(collectRows, result.Rows)
		itemsDeQueued++
	}
	close(resultChan)

	atomic.AddUint64(&f.stats.analysisTime, uint64(time.Since(analysisStart)))

	var deleteKeys [][]byte
	if len(batch.InternalOps) > 0 {
		// add the internal ops
		updateInternalRows := make([]index.IndexRow, 0, len(batch.InternalOps))
		for internalKey, internalValue := range batch.InternalOps {
			if internalValue == nil {
				// delete
				deleteInternalRow := NewInternalRow([]byte(internalKey), nil)
				deleteKeys = append(deleteKeys, deleteInternalRow.Key())
			} else {
				updateInternalRow := NewInternalRow([]byte(internalKey), internalValue)
				updateInternalRows = append(updateInternalRows, updateInternalRow)
			}
		}
		collectRows = append(collectRows, updateInternalRows)
	}

	inflightItems := make([]*InFlightItem, 0, len(batch.IndexOps))
	for docID, doc := range batch.IndexOps {
		if doc != nil {
			inflightItems = append(inflightItems,
				&InFlightItem{[]byte(docID), doc.Number})
		} else {
			inflightItems = append(inflightItems,
				&InFlightItem{[]byte(docID), 0})
		}
	}

	indexStart := time.Now()

	// start a writer for this batch
	var kvwriter store.KVWriter
	kvwriter, err = f.store.Writer()
	if err != nil {
		return
	}

	var dictionaryDeltas map[string]int64
	dictionaryDeltas, err = f.batchRows(kvwriter, collectRows, deleteKeys)
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&f.stats.errors, 1)
		return
	}

	f.compensator.MutateBatch(inflightItems, lastDocNumber)

	err = kvwriter.Close()

	f.lookuper.NotifyBatch(inflightItems)
	f.dictUpdater.NotifyBatch(dictionaryDeltas)

	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))

	if err == nil {
		atomic.AddUint64(&f.stats.updates, docsUpdated)
		atomic.AddUint64(&f.stats.deletes, docsDeleted)
		atomic.AddUint64(&f.stats.batches, 1)
		atomic.AddUint64(&f.stats.numPlainTextBytesIndexed, numPlainTextBytes)
	} else {
		atomic.AddUint64(&f.stats.errors, 1)
	}

	return
}

func (f *Firestorm) SetInternal(key, val []byte) (err error) {
	internalRow := NewInternalRow(key, val)
	var writer store.KVWriter
	writer, err = f.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	wb := writer.NewBatch()
	wb.Set(internalRow.Key(), internalRow.Value())

	return writer.ExecuteBatch(wb)
}

func (f *Firestorm) DeleteInternal(key []byte) (err error) {
	internalRow := NewInternalRow(key, nil)
	var writer store.KVWriter
	writer, err = f.store.Writer()
	if err != nil {
		return
	}
	defer func() {
		if cerr := writer.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	wb := writer.NewBatch()
	wb.Delete(internalRow.Key())

	return writer.ExecuteBatch(wb)
}

func (f *Firestorm) DumpAll() chan interface{} {
	rv := make(chan interface{})
	go func() {
		defer close(rv)

		// start an isolated reader for use during the dump
		kvreader, err := f.store.Reader()
		if err != nil {
			rv <- err
			return
		}
		defer func() {
			cerr := kvreader.Close()
			if cerr != nil {
				rv <- cerr
			}
		}()

		err = f.dumpPrefix(kvreader, rv, nil)
		if err != nil {
			rv <- err
			return
		}
	}()
	return rv
}

func (f *Firestorm) DumpDoc(docID string) chan interface{} {
	rv := make(chan interface{})
	go func() {
		defer close(rv)

		// start an isolated reader for use during the dump
		kvreader, err := f.store.Reader()
		if err != nil {
			rv <- err
			return
		}
		defer func() {
			cerr := kvreader.Close()
			if cerr != nil {
				rv <- cerr
			}
		}()

		err = f.dumpDoc(kvreader, rv, []byte(docID))
		if err != nil {
			rv <- err
			return
		}
	}()
	return rv
}

func (f *Firestorm) DumpFields() chan interface{} {
	rv := make(chan interface{})
	go func() {
		defer close(rv)

		// start an isolated reader for use during the dump
		kvreader, err := f.store.Reader()
		if err != nil {
			rv <- err
			return
		}
		defer func() {
			cerr := kvreader.Close()
			if cerr != nil {
				rv <- cerr
			}
		}()

		err = f.dumpPrefix(kvreader, rv, FieldKeyPrefix)
		if err != nil {
			rv <- err
			return
		}
	}()
	return rv
}

func (f *Firestorm) Reader() (index.IndexReader, error) {
	return newFirestormReader(f)
}

func (f *Firestorm) Stats() json.Marshaler {
	return f.stats
}

func (f *Firestorm) StatsMap() map[string]interface{} {
	return f.stats.statsMap()
}

func (f *Firestorm) Wait(timeout time.Duration) error {
	return f.dictUpdater.waitTasksDone(timeout)
}

func (f *Firestorm) Advanced() (store.KVStore, error) {
	return f.store, nil
}

func init() {
	registry.RegisterIndexType(Name, NewFirestorm)
}
