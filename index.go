//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

// A Batch groups together multiple Index and Delete
// operations you would like performed at the same
// time.
type Batch struct {
	index    Index
	internal *index.Batch
}

// Index adds the specified index operation to the
// batch.  NOTE: the bleve Index is not updated
// until the batch is executed.
func (b Batch) Index(id string, data interface{}) error {
	doc := document.NewDocument(id)
	err := b.index.Mapping().mapDocument(doc, data)
	if err != nil {
		return err
	}
	b.internal.Update(doc)
	return nil
}

// Delete adds the specified delete operation to the
// batch.  NOTE: the bleve Index is not updated until
// the batch is executed.
func (b Batch) Delete(id string) {
	b.internal.Delete(id)
}

// SetInternal adds the specified set internal
// operation to the batch. NOTE: the bleve Index is
// not updated until the batch is executed.
func (b Batch) SetInternal(key, val []byte) {
	b.internal.SetInternal(key, val)
}

// SetInternal adds the specified delete internal
// operation to the batch. NOTE: the bleve Index is
// not updated until the batch is executed.
func (b Batch) DeleteInternal(key []byte) {
	b.internal.DeleteInternal(key)
}

func (b Batch) Size() int {
	return len(b.internal.IndexOps) + len(b.internal.InternalOps)
}

// An Index implements all the indexing and searching
// capabilities of bleve.  An Index can be created
// using the New() and Open() methods.
type Index interface {
	Index(id string, data interface{}) error
	Delete(id string) error

	NewBatch() *Batch
	Batch(b *Batch) error

	Document(id string) (*document.Document, error)
	DocCount() (uint64, error)

	Search(req *SearchRequest) (*SearchResult, error)

	Fields() ([]string, error)

	FieldDict(field string) (index.FieldDict, error)
	FieldDictRange(field string, startTerm []byte, endTerm []byte) (index.FieldDict, error)
	FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error)

	DumpAll() chan interface{}
	DumpDoc(id string) chan interface{}
	DumpFields() chan interface{}

	Close() error

	Mapping() *IndexMapping

	Stats() *IndexStat

	GetInternal(key []byte) ([]byte, error)
	SetInternal(key, val []byte) error
	DeleteInternal(key []byte) error

	Advanced() (index.Index, store.KVStore, error)
}

// A Classifier is an interface describing any object
// which knows how to identify its own type.
type Classifier interface {
	Type() string
}

// New index at the specified path, must not exist.
// The provided mapping will be used for all
// Index/Search operations.
func New(path string, mapping *IndexMapping) (Index, error) {
	return newIndexUsing(path, mapping, Config.DefaultKVStore, nil)
}

// NewUsing creates index at the specified path,
// which must not already exist.
// The provided mapping will be used for all
// Index/Search operations.
// The specified kvstore implemenation will be used
// and the provided kvconfig will be passed to its
// constructor.
func NewUsing(path string, mapping *IndexMapping, kvstore string, kvconfig map[string]interface{}) (Index, error) {
	return newIndexUsing(path, mapping, kvstore, kvconfig)
}

// Open index at the specified path, must exist.
// The mapping used when it was created will be used for all Index/Search operations.
func Open(path string) (Index, error) {
	return openIndexUsing(path, nil)
}

// OpenUsing opens index at the specified path, must exist.
// The mapping used when it was created will be used for all Index/Search operations.
// The provided runtimeConfig can override settings
// persisted when the kvstore was created.
func OpenUsing(path string, runtimeConfig map[string]interface{}) (Index, error) {
	return openIndexUsing(path, runtimeConfig)
}
