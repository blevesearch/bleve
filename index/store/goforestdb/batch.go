// +build forestdb

package goforestdb

type ForestDBBatch struct {
	store *ForestDBStore
	keys  [][]byte
	vals  [][]byte
}

func newForestDBBatch(store *ForestDBStore) *ForestDBBatch {
	rv := ForestDBBatch{
		store: store,
		keys:  make([][]byte, 0),
		vals:  make([][]byte, 0),
	}
	return &rv
}

func (i *ForestDBBatch) Set(key, val []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, val)
}

func (i *ForestDBBatch) Delete(key []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, nil)
}

func (i *ForestDBBatch) Execute() error {
	for index, key := range i.keys {
		val := i.vals[index]
		if val == nil {
			i.store.Delete(key)
		} else {
			i.store.Set(key, val)
		}
	}
	return nil
}

func (i *ForestDBBatch) Close() error {
	return nil
}
