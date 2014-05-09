package store

type KVBatch interface {
	Set(key, val []byte)
	Delete(key []byte)
	Execute() error
	Close() error
}

type KVIterator interface {
	SeekFirst()
	Seek([]byte)
	Next()

	Current() ([]byte, []byte, bool)
	Key() []byte
	Value() []byte
	Valid() bool

	Close()
}

type KVStore interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error
	Delete(key []byte) error
	Commit() error
	Close() error

	Iterator(key []byte) KVIterator
	NewBatch() KVBatch
}
