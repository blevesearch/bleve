package redis

import (
	"fmt"

	store "github.com/blevesearch/upsidedown_store_api"
)

// Reader implements the KVReader interface for a Redis backend.
type Reader struct {
	store *Store
}

// Get returns the value associated with the key
// If the key does not exist, nil is returned.
// The caller owns the bytes returned.
func (r Reader) Get(key []byte) ([]byte, error) {

	// TODO Implement retrieving a single key

	return nil, fmt.Errorf("Not implemented")
}

// MultiGet retrieves multiple values in one call.
func (r Reader) MultiGet(keys [][]byte) ([][]byte, error) {

	// TODO implement retrieving the keys
	return nil, fmt.Errorf("Not implemented")
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r Reader) PrefixIterator(prefix []byte) store.KVIterator {
	return Iterator{store: r.store, prefix: prefix}
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r Reader) RangeIterator(start, end []byte) store.KVIterator {
	return Iterator{store: r.store, start: start, end: end}
}

// Close closes the Reader.
func (r Reader) Close() error {

	// TODO Check wether reader must be closed and implement accordingly.
	return fmt.Errorf("Not implemented/checked")
}
