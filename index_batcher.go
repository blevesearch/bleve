package bleve

import (
	"fmt"
	"sync"
	"time"
)

// bleveIndex is an alias that allows the IndexBatcher to embed a Index
type bleveIndex Index

// IndexBatcher can be wrapped around a Index to aggregate operations
// in a concurrent / parallel context for increased throughput.
type IndexBatcher struct {
	bleveIndex

	period time.Duration
	closer chan bool

	// lock is used to protect applying / resetting the batch, updating /
	// replacing the result, and signalling / replacing the signal channel from
	// the batch loop.  Elsewhere it is used for getting a reference to the
	// current result, getting a reference to the current signal channel,
	// and adding operations to the batch.
	lock   sync.Mutex
	batch  *Batch
	result *rslt
	signal chan bool
}

// rslt is used as a silly double pointer for errors
type rslt struct {
	err error
}

// NewIndexBatcher returns an index that will aggregate and fire modifying
// requests as a batch every period time. All other Index methods are
// passed straight through to the underlying index. Period time should be
// tuned to the underlying KVStore, the concurrent load, latency requirements,
// and the documents / document mappings being used. Single digit (7~8)
// milliseconds is a reasonable place to start.
func NewIndexBatcher(index Index, period time.Duration) Index {
	ib := &IndexBatcher{
		bleveIndex: index,

		batch:  index.NewBatch(),
		period: period,
		closer: make(chan bool),
		signal: make(chan bool),
		result: &rslt{},
	}

	go ib.batchloop()
	return ib
}

// batchloop processes batches every period and implements a clean close
// operation
func (ib *IndexBatcher) batchloop() {
	t := time.NewTicker(ib.period)

BatchLoop:
	for {
		select {
		case <-t.C:
			ib.lock.Lock()
			func() {
				defer func() {
					if r := recover(); r != nil {
						ib.result.err = fmt.Errorf("IndexBatcher caught a panic: %v", r)
					}
				}()
				ib.result.err = ib.Batch(ib.batch)
			}()
			ib.batch.Reset()
			ib.result = &rslt{}
			close(ib.signal)
			ib.signal = make(chan bool)
			ib.lock.Unlock()
		case <-ib.closer:
			break BatchLoop
		}
	}

	t.Stop()
	ib.result.err = fmt.Errorf("IndexBatcher has been closed")
	close(ib.signal)
}

// Close stops the batcher returning an error to currently waiting operations
// and closes the underlying Index
func (ib *IndexBatcher) Close() error {
	ib.closer <- true
	return ib.bleveIndex.Close()
}

// Index the object with the specified identifier. May hold the operation for up
// to ib.period time before executing in a batch.
func (ib *IndexBatcher) Index(id string, data interface{}) error {
	var result *rslt
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	err := ib.batch.Index(id, data)
	ib.lock.Unlock()

	if err != nil {
		return err
	}

	<-signal
	return result.err
}

// Delete entries for the specified identifier from the index. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) Delete(id string) error {
	var result *rslt
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.Delete(id)
	ib.lock.Unlock()

	<-signal
	return result.err
}

// SetInternal mappings directly in the kvstore. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) SetInternal(key, val []byte) error {
	var result *rslt
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.SetInternal(key, val)
	ib.lock.Unlock()

	<-signal
	return result.err
}

// DeleteInternal mappings directly from the kvstore. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) DeleteInternal(key []byte) error {
	var result *rslt
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.DeleteInternal(key)
	ib.lock.Unlock()

	<-signal
	return result.err
}
