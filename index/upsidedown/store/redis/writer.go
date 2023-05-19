package redis

import (
	"fmt"

	store "github.com/blevesearch/upsidedown_store_api"
)

// Writer is used to write values to Redis.
type Writer struct {
	store *Store
}

// NewBatch returns a KVBatch for performing batch operations on this kvstore
func (w Writer) NewBatch() store.KVBatch {
	// TODO Implement store.KVBatch for Redis, create one and return it
	return nil
}

// NewBatchEx returns a KVBatch and an associated byte array
// that's pre-sized based on the KVBatchOptions.  The caller can
// use the returned byte array for keys and values associated with
// the batch.  Once the batch is either executed or closed, the
// associated byte array should no longer be accessed by the
// caller.
func (w Writer) NewBatchEx(store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	// TODO Implement. Have an eye on KVBatchOptions and how to translate them to
	// Redis
	return nil, nil, fmt.Errorf("Not yet implemented")
}

// ExecuteBatch will execute the KVBatch, the provided KVBatch **MUST** have
// been created by the same KVStore (though not necessarily the same KVWriter)
// Batch execution is atomic, either all the operations or none will be performed
func (w Writer) ExecuteBatch(batch store.KVBatch) error {
	// TODO Implement according to the above specification
	// Might be an actual problem, since afaik, there are no tranactions which can be rolled back in Redis.
	return fmt.Errorf("Not yet implemented")
}

// Close closes the writer
func (w Writer) Close() error {
	// TODO unclear. No clue wether Redis has a way to rollback from a MULTI statement
	return fmt.Errorf("Not yet implemented")
}
