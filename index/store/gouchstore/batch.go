//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
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
	doc := gouchstore.Document{ID: id, Body: val}
	docInfo := gouchstore.DocumentInfo{ID: id, ContentMeta: gouchstore.DOC_IS_COMPRESSED}
	gb.bulk.Set(&docInfo, &doc)
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
