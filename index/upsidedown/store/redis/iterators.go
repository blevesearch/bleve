package redis

import "bytes"

// Iterator statisfies both the PrefixIterator and the RangeIterator interface.
// As PrefixIterator, it will visit all K/V pairs with the provided prefix.
// As RangeIterator, it will visit all K/V pairs between start and end.
type Iterator struct {
	store *Store
	// Prefix for usage as a PrefixIterator
	prefix []byte
	start  []byte
	end    []byte
	valid  bool
	// Stores the current key
	key []byte
	// Stores the current value
	value []byte
}

// Blatantly copied from bolt store
func (i *Iterator) updateValid() {
	i.valid = (i.key != nil)
	if i.valid {
		if i.prefix != nil {
			i.valid = bytes.HasPrefix(i.key, i.prefix)
		} else if i.end != nil {
			i.valid = bytes.Compare(i.key, i.end) < 0
		}
	}
}

// Current returns Key(),Value(),Valid() in a single operation
func (i Iterator) Current() ([]byte, []byte, bool) {
	return i.key, i.value, true
}

// Key returns the key pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i Iterator) Key() []byte {
	return i.key
}

// Value returns the value pointed to by the iterator
// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
// Continued use after that requires that they be copied.
func (i Iterator) Value() []byte {
	return i.value
}

// Next will advance the iterator to the next key
func (i Iterator) Next() {
	// TODO Advance iterator to the next key
	// Depending on
	panic("Not implemented")
}

// Seek will advance the iterator to the specified key
func (i Iterator) Seek(key []byte) {
	// TODO Advance the iterator to the specified key
	// Depending on wether you have a prefix or a range, you need to check this first
	// Also, do not forget to call updateValid()
	panic("Not implemented")
}

// Valid returns whether or not the iterator is in a valid state
func (i Iterator) Valid() bool {
	i.updateValid()
	return i.valid
}

// Close closes the iterator
func (i Iterator) Close() error {
	// TODO check if anything needs to be done to clean up the iterator on the server side.
	return nil
}
