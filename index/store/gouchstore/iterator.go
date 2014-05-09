package gouchstore

import (
	"fmt"

	"github.com/mschoch/gouchstore"
)

type GouchstoreIterator struct {
	store     *GouchstoreStore
	valid     bool
	curr      *gouchstore.DocumentInfo
	diChan    chan *gouchstore.DocumentInfo
	closeChan chan bool
}

func newGouchstoreIterator(store *GouchstoreStore) *GouchstoreIterator {
	rv := GouchstoreIterator{
		store: store,
	}
	return &rv
}

func (gi *GouchstoreIterator) cleanupExistingIterator() {
	if gi.closeChan != nil {
		close(gi.closeChan)
		alive := true
		for alive {
			_, alive = <-gi.diChan
		}
		gi.closeChan = nil
	}
}

func (gi *GouchstoreIterator) SeekFirst() {
	gi.Seek([]byte{})
}

func (gi *GouchstoreIterator) Seek(key []byte) {
	gi.cleanupExistingIterator()
	gi.curr = nil
	gi.diChan = make(chan *gouchstore.DocumentInfo)
	gi.closeChan = make(chan bool)

	wtCallback := func(gouchstore *gouchstore.Gouchstore, depth int, documentInfo *gouchstore.DocumentInfo, key []byte, subTreeSize uint64, reducedValue []byte, userContext interface{}) error {

		if documentInfo != nil && documentInfo.Deleted == false {
			select {
			case gi.diChan <- documentInfo:
				gi.valid = true
			case <-gi.closeChan:
				return fmt.Errorf("seek aborted")
			}
		}
		return nil
	}
	go func() {
		gi.store.db.WalkIdTree(string(key), "", wtCallback, nil)
		close(gi.diChan)
	}()
	gi.curr = <-gi.diChan
}

func (gi *GouchstoreIterator) Current() ([]byte, []byte, bool) {
	if gi.Valid() {
		return gi.Key(), gi.Value(), true
	}
	return nil, nil, false
}

func (gi *GouchstoreIterator) Next() {
	gi.curr = <-gi.diChan
	if gi.curr == nil {
		gi.valid = false
	}
}

func (gi *GouchstoreIterator) Key() []byte {
	if gi.curr != nil {
		return []byte(gi.curr.ID)
	}
	return nil
}

func (gi *GouchstoreIterator) Value() []byte {
	if gi.curr != nil {
		doc, err := gi.store.db.DocumentByDocumentInfo(gi.curr)
		if err == nil {
			return doc.Body
		}
	}
	return nil
}

func (gi *GouchstoreIterator) Valid() bool {
	return gi.valid
}

func (gi *GouchstoreIterator) Close() {
	gi.cleanupExistingIterator()
}
