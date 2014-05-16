package gouchstore

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/mschoch/gouchstore"
)

type GouchstoreStore struct {
	path string
	db   *gouchstore.Gouchstore
}

func Open(path string) (*GouchstoreStore, error) {
	rv := GouchstoreStore{
		path: path,
	}

	var err error
	rv.db, err = gouchstore.Open(path, gouchstore.OPEN_CREATE)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (gs *GouchstoreStore) Get(key []byte) ([]byte, error) {
	var docInfo gouchstore.DocumentInfo
	err := gs.db.DocumentInfoByIdNoAlloc(string(key), &docInfo)
	if err != nil && err.Error() != "document not found" {
		return nil, err
	}
	if err != nil && err.Error() == "document not found" {
		return nil, nil
	}
	var doc gouchstore.Document
	if !docInfo.Deleted {
		err := gs.db.DocumentByIdNoAlloc(string(key), &doc)
		if err != nil {
			return nil, err
		}
		return doc.Body, nil
	}
	return nil, nil
}

func (gs *GouchstoreStore) Set(key, val []byte) error {
	doc := gouchstore.NewDocument(string(key), val)
	docInfo := gouchstore.NewDocumentInfo(string(key))
	return gs.db.SaveDocument(doc, docInfo)
}

func (gs *GouchstoreStore) Delete(key []byte) error {
	doc := gouchstore.NewDocument(string(key), nil)
	docInfo := gouchstore.NewDocumentInfo(string(key))
	docInfo.Deleted = true
	return gs.db.SaveDocument(doc, docInfo)
}

func (gs *GouchstoreStore) Commit() error {
	return gs.db.Commit()
}

func (gs *GouchstoreStore) Close() error {
	return gs.db.Close()
}

func (gs *GouchstoreStore) Iterator(key []byte) store.KVIterator {
	rv := newGouchstoreIterator(gs)
	rv.Seek(key)
	return rv
}

func (gs *GouchstoreStore) NewBatch() store.KVBatch {
	return newGouchstoreBatch(gs)
}
