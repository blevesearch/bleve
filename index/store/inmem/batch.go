package inmem

type InMemBatch struct {
	store *InMemStore
	keys  [][]byte
	vals  [][]byte
}

func newInMemBatch(store *InMemStore) *InMemBatch {
	rv := InMemBatch{
		store: store,
		keys:  make([][]byte, 0),
		vals:  make([][]byte, 0),
	}
	return &rv
}

func (i *InMemBatch) Set(key, val []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, val)
}

func (i *InMemBatch) Delete(key []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, nil)
}

func (i *InMemBatch) Execute() error {
	for index, key := range i.keys {
		val := i.vals[index]
		if val == nil {
			i.store.list.Delete(string(key))
		} else {
			i.store.Set(key, val)
		}
	}
	return nil
}

func (i *InMemBatch) Close() error {
	return nil
}
