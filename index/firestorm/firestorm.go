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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const Name = "firestorm"

var UnsafeBatchUseDetected = fmt.Errorf("bleve.Batch is NOT thread-safe, modification after execution detected")

type Firestorm struct {
	storeName        string
	storeConfig      map[string]interface{}
	store            store.KVStore
	compensator      *Compensator
	analysisQueue    *index.AnalysisQueue
	fieldCache       *index.FieldCache
	highDocNumber    uint64
	docCount         *uint64
	garbageCollector *GarbageCollector
	lookuper         *Lookuper
	dictUpdater      *DictUpdater
	stats            *indexStat
}

func NewFirestorm(storeName string, storeConfig map[string]interface{}, analysisQueue *index.AnalysisQueue) (index.Index, error) {
	initialCount := uint64(0)
	rv := Firestorm{
		storeName:     storeName,
		storeConfig:   storeConfig,
		compensator:   NewCompensator(),
		analysisQueue: analysisQueue,
		fieldCache:    index.NewFieldCache(),
		docCount:      &initialCount,
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
	count := atomic.LoadUint64(f.docCount)
	return count, nil

}

func (f *Firestorm) Update(doc *document.Document) (err error) {

	// assign this document a number
	doc.Number = atomic.AddUint64(&f.highDocNumber, 1)

	// do analysis before acquiring write lock
	analysisStart := time.Now()
	resultChan := make(chan *index.AnalysisResult)
	aw := index.NewAnalysisWork(f, doc, resultChan)

	// put the work on the queue
	go f.analysisQueue.Queue(aw)

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
	dictionaryDeltas, err = f.batchRows(kvwriter, result.Rows, nil)
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&f.stats.errors, 1)
		return
	}

	f.compensator.Mutate([]byte(doc.ID), doc.Number)
	f.lookuper.Notify(doc.Number, []byte(doc.ID))
	f.dictUpdater.NotifyBatch(dictionaryDeltas)

	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))
	return
}

func (f *Firestorm) Delete(id string) error {
	indexStart := time.Now()
	f.compensator.Mutate([]byte(id), 0)
	f.lookuper.Notify(0, []byte(id))
	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))
	return nil
}

func (f *Firestorm) batchRows(writer store.KVWriter, rows []index.IndexRow, deleteKeys [][]byte) (map[string]int64, error) {

	// prepare batch
	wb := writer.NewBatch()

	var kbuf []byte
	var vbuf []byte

	prepareBuf := func(buf []byte, sizeNeeded int) []byte {
		if cap(buf) < sizeNeeded {
			return make([]byte, sizeNeeded, sizeNeeded+128)
		}
		return buf[0:sizeNeeded]
	}

	dictionaryDeltas := make(map[string]int64)
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

		kbuf = prepareBuf(kbuf, row.KeySize())
		klen, err := row.KeyTo(kbuf)
		if err != nil {
			return nil, err
		}

		vbuf = prepareBuf(vbuf, row.ValueSize())
		vlen, err := row.ValueTo(vbuf)
		if err != nil {
			return nil, err
		}

		wb.Set(kbuf[0:klen], vbuf[0:vlen])
	}

	for _, dk := range deleteKeys {
		wb.Delete(dk)
	}

	// write out the batch
	err := writer.ExecuteBatch(wb)
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
	for _, doc := range batch.IndexOps {
		if doc != nil {
			doc.Number = firstDocNumber // actually assign doc numbers here
			firstDocNumber++
			docsUpdated++
		} else {
			docsDeleted++
		}
	}

	var detectedUnsafeMutex sync.RWMutex
	detectedUnsafe := false

	go func() {
		sofar := uint64(0)
		for _, doc := range batch.IndexOps {
			if doc != nil {
				sofar++
				if sofar > docsUpdated {
					detectedUnsafeMutex.Lock()
					detectedUnsafe = true
					detectedUnsafeMutex.Unlock()
					return
				}
				aw := index.NewAnalysisWork(f, doc, resultChan)
				// put the work on the queue
				f.analysisQueue.Queue(aw)
			}
		}
	}()

	allRows := make([]index.IndexRow, 0, 1000)
	// wait for the result
	var itemsDeQueued uint64
	for itemsDeQueued < docsUpdated {
		result := <-resultChan
		allRows = append(allRows, result.Rows...)
		itemsDeQueued++
	}
	close(resultChan)

	detectedUnsafeMutex.RLock()
	defer detectedUnsafeMutex.RUnlock()
	if detectedUnsafe {
		return UnsafeBatchUseDetected
	}

	atomic.AddUint64(&f.stats.analysisTime, uint64(time.Since(analysisStart)))

	deleteKeys := make([][]byte, 0)
	// add the internal ops
	for internalKey, internalValue := range batch.InternalOps {
		if internalValue == nil {
			// delete
			deleteInternalRow := NewInternalRow([]byte(internalKey), nil)
			deleteKeys = append(deleteKeys, deleteInternalRow.Key())
		} else {
			updateInternalRow := NewInternalRow([]byte(internalKey), internalValue)
			allRows = append(allRows, updateInternalRow)
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
	dictionaryDeltas, err = f.batchRows(kvwriter, allRows, deleteKeys)
	if err != nil {
		_ = kvwriter.Close()
		atomic.AddUint64(&f.stats.errors, 1)
		return
	}

	f.compensator.MutateBatch(batch.IndexOps, lastDocNumber)
	for docID, doc := range batch.IndexOps {
		if doc != nil {
			f.lookuper.Notify(doc.Number, []byte(doc.ID))
		} else {
			f.lookuper.Notify(0, []byte(docID))
		}
	}
	f.dictUpdater.NotifyBatch(dictionaryDeltas)

	err = kvwriter.Close()
	atomic.AddUint64(&f.stats.indexTime, uint64(time.Since(indexStart)))

	if err == nil {
		atomic.AddUint64(&f.stats.updates, docsUpdated)
		atomic.AddUint64(&f.stats.deletes, docsDeleted)
		atomic.AddUint64(&f.stats.batches, 1)
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

func (f *Firestorm) Wait(timeout time.Duration) error {
	return f.dictUpdater.waitTasksDone(timeout)
}

func init() {
	registry.RegisterIndexType(Name, NewFirestorm)
}
