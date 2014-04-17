//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package upside_down

import (
	"bytes"

	"github.com/jmhodges/levigo"

	"github.com/couchbaselabs/bleve/index"
)

type UpsideDownCouchTermFieldReader struct {
	index    *UpsideDownCouch
	iterator *levigo.Iterator
	count    uint64
	term     []byte
	field    uint16
}

func newUpsideDownCouchTermFieldReader(index *UpsideDownCouch, term []byte, field uint16) (*UpsideDownCouchTermFieldReader, error) {
	ro := defaultReadOptions()
	it := index.db.NewIterator(ro)

	tfr := NewTermFrequencyRow(term, field, "", 0, 0)
	it.Seek(tfr.Key())

	var count uint64 = 0
	if it.Valid() {
		if bytes.Equal(it.Key(), tfr.Key()) {
			tfr = ParseFromKeyValue(it.Key(), it.Value()).(*TermFrequencyRow)
			count = tfr.freq
		}

	} else {
		return nil, it.GetError()
	}

	return &UpsideDownCouchTermFieldReader{
		index:    index,
		iterator: it,
		count:    count,
		term:     term,
		field:    field,
	}, nil
}

func (r *UpsideDownCouchTermFieldReader) Count() uint64 {
	return r.count
}

func (r *UpsideDownCouchTermFieldReader) Next() (*index.TermFieldDoc, error) {
	r.iterator.Next()
	if r.iterator.Valid() {
		tfr := NewTermFrequencyRow(r.term, r.field, "", 0, 0)
		if !bytes.HasPrefix(r.iterator.Key(), tfr.Key()) {
			// end of the line
			return nil, nil
		}
		tfr = ParseFromKeyValue(r.iterator.Key(), r.iterator.Value()).(*TermFrequencyRow)
		return &index.TermFieldDoc{
			ID:      string(tfr.doc),
			Freq:    tfr.freq,
			Norm:    float64(tfr.norm),
			Vectors: r.index.termFieldVectorsFromTermVectors(tfr.vectors),
		}, nil
	} else {
		return nil, r.iterator.GetError()
	}
}

func (r *UpsideDownCouchTermFieldReader) Advance(docId string) (*index.TermFieldDoc, error) {
	tfr := NewTermFrequencyRow(r.term, r.field, docId, 0, 0)
	r.iterator.Seek(tfr.Key())
	if r.iterator.Valid() {
		tfr := NewTermFrequencyRow(r.term, r.field, "", 0, 0)
		if !bytes.HasPrefix(r.iterator.Key(), tfr.Key()) {
			// end of the line
			return nil, nil
		}
		tfr = ParseFromKeyValue(r.iterator.Key(), r.iterator.Value()).(*TermFrequencyRow)
		return &index.TermFieldDoc{
			ID:      string(tfr.doc),
			Freq:    tfr.freq,
			Norm:    float64(tfr.norm),
			Vectors: r.index.termFieldVectorsFromTermVectors(tfr.vectors),
		}, nil
	} else {
		return nil, r.iterator.GetError()
	}
}

func (r *UpsideDownCouchTermFieldReader) Close() {
	r.iterator.Close()
}
