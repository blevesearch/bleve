package gouchstore

import (
	"github.com/mschoch/gouchstore"
)

func kvToDocDocInfo(key, val []byte) (*gouchstore.Document, *gouchstore.DocumentInfo) {
	id := string(key)
	return gouchstore.NewDocument(id, val), gouchstore.NewDocumentInfo(id)
}
