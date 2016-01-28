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
	"math"
	"sync"
	"time"
)

const DefaultGarbageThreshold = 10
const DefaultMaxDocsPerPass = 1000

var DefaultGarbageSleep = 15 * time.Second

type GarbageCollector struct {
	f                *Firestorm
	garbageThreshold int
	garbageSleep     time.Duration
	maxDocsPerPass   int
	quit             chan struct{}

	mutex      sync.RWMutex
	workingSet map[uint64][]byte
	closeWait  sync.WaitGroup
}

func NewGarbageCollector(f *Firestorm) *GarbageCollector {
	rv := GarbageCollector{
		f:                f,
		garbageThreshold: DefaultGarbageThreshold,
		garbageSleep:     DefaultGarbageSleep,
		maxDocsPerPass:   DefaultMaxDocsPerPass,
		quit:             make(chan struct{}),
		workingSet:       make(map[uint64][]byte),
	}
	return &rv
}

func (gc *GarbageCollector) Notify(docNum uint64, docId []byte) {
	gc.mutex.Lock()
	defer gc.mutex.Unlock()
	gc.workingSet[docNum] = docId
}

func (gc *GarbageCollector) Start() {
	gc.closeWait.Add(1)
	go gc.run()
}

func (gc *GarbageCollector) Stop() {
	close(gc.quit)
	gc.closeWait.Wait()
}

func (gc *GarbageCollector) run() {
	tick := time.Tick(gc.garbageSleep)
	for {
		select {
		case <-gc.quit:
			logger.Printf("garbage collector asked to quit")
			gc.closeWait.Done()
			return
		case <-tick:
			logger.Printf("garbage collector ticked")
			garbageSize := gc.f.compensator.GarbageCount()
			docSize, err := gc.f.DocCount()
			if err != nil {
				logger.Printf("garbage collector error getting doc count: %v", err)
				continue
			}
			if docSize == 0 {
				continue
			}
			garbageRatio := int(uint64(garbageSize) / docSize)
			if garbageRatio > gc.garbageThreshold {
				gc.cleanup()
			} else {
				logger.Printf("garbage ratio only %d, waiting", garbageRatio)
			}

		}
	}
}

func (gc *GarbageCollector) NextBatch(n int) []uint64 {
	gc.mutex.RLock()
	defer gc.mutex.RUnlock()

	rv := make([]uint64, 0, n)
	i := 0
	for k := range gc.workingSet {
		rv = append(rv, k)
		i++
		if i > n {
			break
		}
	}

	return rv
}

func (gc *GarbageCollector) cleanup() {
	logger.Printf("garbage collector starting")
	// get list of deleted doc numbers to work on this pass
	deletedDocNumsList := gc.NextBatch(gc.maxDocsPerPass) //gc.f.deletedDocNumbers.Keys(gc.maxDocsPerPass)
	logger.Printf("found %d doc numbers to cleanup", len(deletedDocNumsList))

	// put these documents numbers in a map, for faster checking
	// and for organized keys to be deleted
	deletedDocNums := make(map[uint64][][]byte)
	for _, deletedDocNum := range deletedDocNumsList {
		deletedDocNums[deletedDocNum] = make([][]byte, 0)
	}

	reader, err := gc.f.store.Reader()
	if err != nil {
		logger.Printf("garbage collector fatal: %v", err)
		return
	}
	defer func() {
		if cerr := reader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	// walk all the term freq rows (where field > 0)
	termFreqStart := TermFreqIteratorStart(0, []byte{ByteSeparator})
	termFreqEnd := TermFreqIteratorStart(math.MaxUint16, []byte{ByteSeparator})

	var tfr TermFreqRow
	dictionaryDeltas := make(map[string]int64)
	err = visitRange(reader, termFreqStart, termFreqEnd, func(key, val []byte) (bool, error) {
		err := tfr.ParseKey(key)
		if err != nil {
			return false, err
		}
		docNum := tfr.DocNum()
		if docNumKeys, deleted := deletedDocNums[docNum]; deleted {
			// this doc number has been deleted, place key into map
			deletedDocNums[docNum] = append(docNumKeys, key)
			if tfr.Field() != 0 {
				drk := tfr.DictionaryRowKey()
				dictionaryDeltas[string(drk)] -= 1
			}
		}
		return true, nil
	})
	if err != nil {
		logger.Printf("garbage collector fatal: %v", err)
		return
	}

	// walk all the stored rows
	var sr StoredRow
	err = visitPrefix(reader, StoredKeyPrefix, func(key, val []byte) (bool, error) {
		err := sr.ParseKey(key)
		if err != nil {
			return false, err
		}
		docNum := sr.DocNum()
		if docNumKeys, deleted := deletedDocNums[docNum]; deleted {
			// this doc number has been deleted, place key into map
			deletedDocNums[docNum] = append(docNumKeys, key)
		}
		return true, nil
	})
	if err != nil {
		logger.Printf("garbage collector fatal: %v", err)
		return
	}

	// now process each doc one at a time
	for docNum, docKeys := range deletedDocNums {

		// delete keys for a doc number
		logger.Printf("deleting keys for %d", docNum)
		// open a writer
		writer, err := gc.f.store.Writer()
		if err != nil {
			_ = writer.Close()
			logger.Printf("garbage collector fatal: %v", err)
			return
		}

		// prepare batch
		wb := writer.NewBatch()

		for _, k := range docKeys {
			wb.Delete(k)
		}

		err = writer.ExecuteBatch(wb)
		if err != nil {
			_ = writer.Close()
			logger.Printf("garbage collector fatal: %v", err)
			return
		}
		logger.Printf("deleted %d keys", len(docKeys))

		// remove it from delete keys list
		docID := gc.workingSet[docNum]
		delete(gc.workingSet, docNum)
		gc.f.compensator.GarbageCollect([]uint64{docNum})

		// now delete the original marker row (field 0)
		tfidrow := NewTermFreqRow(0, nil, docID, docNum, 0, 0, nil)
		markerRowKey := tfidrow.Key()

		markerBatch := writer.NewBatch()
		markerBatch.Delete(markerRowKey)
		err = writer.ExecuteBatch(markerBatch)
		if err != nil {
			logger.Printf("garbage collector fatal: %v", err)
			return
		}
		err = writer.Close()
		if err != nil {
			logger.Printf("garbage collector fatal: %v", err)
			return
		}
	}

	// updating dictionary in one batch
	gc.f.dictUpdater.NotifyBatch(dictionaryDeltas)

	logger.Printf("garbage collector finished")
}
