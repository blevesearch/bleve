package bleve

import (
	"fmt"
	"sync"
	"time"
)

// NOTE As this approach uses carefully orchestrated interactions
// between a mutex, channels, and reference swapping, be sure to carefully
// consider the impact of reordering statements or making changes to the
// patterns present in the code. Specifically, individual operations follow the
// pattern of:
// - Construct empty references for result error, signal channel
// - Acquire lock
// - Assign references to current result error and signal channel
// - Add operation to batch
// - Release lock
// - Wait on signal channel to close
// - Return result error
// While the batch loop follows the pattern on timer event:
// - Acquire lock
// - Execute batch
// - Store result in current result error
// - Make new current result error for use by operations in the next batch
// - Close signal channel so that operations waiting on the completion of this
//   batch will return the result pointed to by the previous result error that
//   they are still holding.
// - Make new current signal channel for use by operations in the next batch
// - Release lock
// This design minimizes the need for allocating channels, uses only one go
// routine for the batching, and doesn't require tracking the number of
// operations waiting on a response or looping to notify each waiting operation.

// bleveIndex is an alias for Index used by the IndexBatcher to avoid a between
// the embedded Index field and the overrideen Index method
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
	result *error
	signal chan bool
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
		result: new(error),
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
						(*ib.result) = fmt.Errorf("IndexBatcher caught a panic: %v", r)
					}
				}()
				(*ib.result) = ib.Batch(ib.batch)
			}()
			ib.batch.Reset()
			ib.result = new(error)
			close(ib.signal)
			ib.signal = make(chan bool)
			ib.lock.Unlock()
		case <-ib.closer:
			break BatchLoop
		}
	}

	t.Stop()
	(*ib.result) = fmt.Errorf("IndexBatcher has been closed")
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
	var result *error
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
	return *result
}

// Delete entries for the specified identifier from the index. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) Delete(id string) error {
	var result *error
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.Delete(id)
	ib.lock.Unlock()

	<-signal
	return *result
}

// SetInternal mappings directly in the kvstore. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) SetInternal(key, val []byte) error {
	var result *error
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.SetInternal(key, val)
	ib.lock.Unlock()

	<-signal
	return *result
}

// DeleteInternal mappings directly from the kvstore. May hold the
// operation for up to ib.period time before executing in a batch.
func (ib *IndexBatcher) DeleteInternal(key []byte) error {
	var result *error
	var signal chan bool
	ib.lock.Lock()
	result = ib.result
	signal = ib.signal
	ib.batch.DeleteInternal(key)
	ib.lock.Unlock()

	<-signal
	return *result
}
