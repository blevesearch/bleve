package gouchstore

import (
	"github.com/mschoch/gouchstore"
)

type GouchstoreBatch struct {
	store *GouchstoreStore
	bulk  gouchstore.BulkWriter
}

func newGouchstoreBatch(store *GouchstoreStore) *GouchstoreBatch {
	rv := GouchstoreBatch{
		store: store,
		bulk:  store.db.Bulk(),
	}
	return &rv
}

func (gb *GouchstoreBatch) Set(key, val []byte) {
	id := string(key)
	doc := &gouchstore.Document{ID: id, Body: val}
	docInfo := &gouchstore.DocumentInfo{ID: id, ContentMeta: gouchstore.DOC_IS_COMPRESSED}
	gb.bulk.Set(docInfo, doc)
}

func (gb *GouchstoreBatch) Delete(key []byte) {
	id := string(key)
	docInfo := &gouchstore.DocumentInfo{ID: id, ContentMeta: gouchstore.DOC_IS_COMPRESSED}
	gb.bulk.Delete(docInfo)
}

func (gb *GouchstoreBatch) Execute() error {
	return gb.bulk.Commit()
}

func (gb *GouchstoreBatch) Close() error {
	return gb.bulk.Close()
}
