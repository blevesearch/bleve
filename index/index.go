//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package index

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/document"
)

var ErrorUnknownStorageType = fmt.Errorf("unknown storage type")

type Index interface {
	Open() error
	Close() error

	DocCount() (uint64, error)

	Update(doc *document.Document) error
	Delete(id string) error
	Batch(batch *Batch) error

	SetInternal(key, val []byte) error
	DeleteInternal(key []byte) error

	DumpAll() chan interface{}
	DumpDoc(id string) chan interface{}
	DumpFields() chan interface{}

	// Reader returns a low-level accessor on the index data. Close it to
	// release associated resources.
	Reader() (IndexReader, error)

	Stats() json.Marshaler

	Analyze(d *document.Document) *AnalysisResult
}

type IndexReader interface {
	TermFieldReader(term []byte, field string) (TermFieldReader, error)

	// DocIDReader returns an iterator over documents which identifiers are
	// greater than or equal to start and smaller than end. The caller must
	// close returned instance to release associated resources.
	DocIDReader(start, end string) (DocIDReader, error)

	FieldDict(field string) (FieldDict, error)

	// FieldDictRange is currently defined to include the start and end terms
	FieldDictRange(field string, startTerm []byte, endTerm []byte) (FieldDict, error)
	FieldDictPrefix(field string, termPrefix []byte) (FieldDict, error)

	Document(id string) (*document.Document, error)
	DocumentFieldTerms(id string) (FieldTerms, error)

	Fields() ([]string, error)

	GetInternal(key []byte) ([]byte, error)

	DocCount() uint64

	Close() error
}

type FieldTerms map[string][]string

type TermFieldVector struct {
	Field          string
	ArrayPositions []uint64
	Pos            uint64
	Start          uint64
	End            uint64
}

type TermFieldDoc struct {
	Term    string
	ID      string
	Freq    uint64
	Norm    float64
	Vectors []*TermFieldVector
}

type TermFieldReader interface {
	Next() (*TermFieldDoc, error)
	Advance(ID string) (*TermFieldDoc, error)
	Count() uint64
	Close() error
}

type DictEntry struct {
	Term  string
	Count uint64
}

type FieldDict interface {
	Next() (*DictEntry, error)
	Close() error
}

// DocIDReader is the interface exposing enumeration of documents identifiers.
// Close the reader to release associated resources.
type DocIDReader interface {
	// Next returns the next document identifier in ascending lexicographic
	// byte order, or io.EOF when the end of the sequence is reached.
	Next() (string, error)

	// Advance resets the iteration to the first identifier greater than or
	// equal to ID. If ID is smaller than the start of the range, the iteration
	// will start there instead. If ID is greater than or equal to the end of
	// the range, Next() call will return io.EOF.
	Advance(ID string) (string, error)
	Close() error
}

type Batch struct {
	IndexOps    map[string]*document.Document
	InternalOps map[string][]byte
}

func NewBatch() *Batch {
	return &Batch{
		IndexOps:    make(map[string]*document.Document),
		InternalOps: make(map[string][]byte),
	}
}

func (b *Batch) Update(doc *document.Document) {
	b.IndexOps[doc.ID] = doc
}

func (b *Batch) Delete(id string) {
	b.IndexOps[id] = nil
}

func (b *Batch) SetInternal(key, val []byte) {
	b.InternalOps[string(key)] = val
}

func (b *Batch) DeleteInternal(key []byte) {
	b.InternalOps[string(key)] = nil
}

func (b *Batch) String() string {
	rv := fmt.Sprintf("Batch (%d ops, %d internal ops)\n", len(b.IndexOps), len(b.InternalOps))
	for k, v := range b.IndexOps {
		if v != nil {
			rv += fmt.Sprintf("\tINDEX - '%s'\n", k)
		} else {
			rv += fmt.Sprintf("\tDELETE - '%s'\n", k)
		}
	}
	for k, v := range b.InternalOps {
		if v != nil {
			rv += fmt.Sprintf("\tSET INTERNAL - '%s'\n", k)
		} else {
			rv += fmt.Sprintf("\tDELETE INTERNAL - '%s'\n", k)
		}
	}
	return rv
}

func (b *Batch) Reset() {
	for k, _ := range b.IndexOps {
		delete(b.IndexOps, k)
	}
	for k, _ := range b.InternalOps {
		delete(b.InternalOps, k)
	}
}
