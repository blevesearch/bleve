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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultDictUpdateThreshold = 10

var DefaultDictUpdateSleep = 1 * time.Second

type DictUpdater struct {
	batchesStarted uint64
	batchesFlushed uint64

	f               *Firestorm
	dictUpdateSleep time.Duration
	quit            chan struct{}
	incoming        chan map[string]int64

	mutex      sync.RWMutex
	workingSet map[string]int64
	closeWait  sync.WaitGroup
}

func NewDictUpdater(f *Firestorm) *DictUpdater {
	rv := DictUpdater{
		f:               f,
		dictUpdateSleep: DefaultDictUpdateSleep,
		workingSet:      make(map[string]int64),
		batchesStarted:  1,
		quit:            make(chan struct{}),
		incoming:        make(chan map[string]int64, 8),
	}
	return &rv
}

func (d *DictUpdater) Notify(term string, usage int64) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.workingSet[term] += usage
}

func (d *DictUpdater) NotifyBatch(termUsages map[string]int64) {
	d.incoming <- termUsages
}

func (d *DictUpdater) Start() {
	d.closeWait.Add(1)
	go d.runIncoming()
	go d.run()
}

func (d *DictUpdater) Stop() {
	close(d.quit)
	d.closeWait.Wait()
}

func (d *DictUpdater) runIncoming() {
	for {
		select {
		case <-d.quit:
			return
		case termUsages, ok := <-d.incoming:
			if !ok {
				return
			}
			d.mutex.Lock()
			for term, usage := range termUsages {
				d.workingSet[term] += usage
			}
			d.mutex.Unlock()
		}
	}
}

func (d *DictUpdater) run() {
	tick := time.Tick(d.dictUpdateSleep)
	for {
		select {
		case <-d.quit:
			logger.Printf("dictionary updater asked to quit")
			d.closeWait.Done()
			return
		case <-tick:
			logger.Printf("dictionary updater ticked")
			d.update()
		}
	}
}

func (d *DictUpdater) update() {
	d.mutex.Lock()
	oldWorkingSet := d.workingSet
	d.workingSet = make(map[string]int64)
	atomic.AddUint64(&d.batchesStarted, 1)
	d.mutex.Unlock()

	// open a writer
	writer, err := d.f.store.Writer()
	if err != nil {
		_ = writer.Close()
		logger.Printf("dict updater fatal: %v", err)
		return
	}

	// prepare batch
	wb := writer.NewBatch()

	dictionaryTermDelta := make([]byte, 8)
	for term, delta := range oldWorkingSet {
		binary.LittleEndian.PutUint64(dictionaryTermDelta, uint64(delta))
		wb.Merge([]byte(term), dictionaryTermDelta)
	}

	err = writer.ExecuteBatch(wb)
	if err != nil {
		_ = writer.Close()
		logger.Printf("dict updater fatal: %v", err)
		return
	}

	atomic.AddUint64(&d.batchesFlushed, 1)

	_ = writer.Close()
}

// this is not intended to be used publicly, only for unit tests
// which depend on consistency we no longer provide
func (d *DictUpdater) waitTasksDone(dur time.Duration) error {
	initial := atomic.LoadUint64(&d.batchesStarted)
	timeout := time.After(dur)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		// Got a timeout! fail with a timeout error
		case <-timeout:
			flushed := atomic.LoadUint64(&d.batchesFlushed)
			return fmt.Errorf("timeout, %d/%d", initial, flushed)
		// Got a tick, we should check on doSomething()
		case <-tick:
			flushed := atomic.LoadUint64(&d.batchesFlushed)
			if flushed > initial {
				return nil
			}
		}
	}
}
