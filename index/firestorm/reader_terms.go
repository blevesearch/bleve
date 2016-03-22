//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"bytes"
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type firestormTermFieldReader struct {
	r      *firestormReader
	field  uint16
	term   []byte
	prefix []byte
	count  uint64
	i      store.KVIterator
}

func newFirestormTermFieldReader(r *firestormReader, field uint16, term []byte) (index.TermFieldReader, error) {
	dictionaryKey := DictionaryRowKey(field, term)
	dictionaryValue, err := r.r.Get(dictionaryKey)
	if err != nil {
		return nil, err
	}

	prefix := TermFreqIteratorStart(field, term)
	logger.Printf("starting term freq iterator at: '%s' - % x", prefix, prefix)
	i := r.r.PrefixIterator(prefix)
	rv := firestormTermFieldReader{
		r:      r,
		field:  field,
		term:   term,
		prefix: prefix,
		i:      i,
	}

	// NOTE: in firestorm the dictionary row is advisory in nature
	// it *may* tell us the correct out
	// if this record does not exist, it DOES not mean that there is no
	// usage, we must scan the term frequencies to be sure
	if dictionaryValue != nil {
		dictionaryRow, err := NewDictionaryRowKV(dictionaryKey, dictionaryValue)
		if err != nil {
			return nil, err
		}
		rv.count = dictionaryRow.Count()
	}

	atomic.AddUint64(&r.f.stats.termSearchersStarted, uint64(1))
	return &rv, nil
}

func (r *firestormTermFieldReader) Next() (*index.TermFieldDoc, error) {
	if r.i != nil {
		key, val, valid := r.i.Current()
		for valid {
			logger.Printf("see key: '%s' - % x", key, key)
			tfrsByDocNum := make(map[uint64]*TermFreqRow)
			tfr, err := NewTermFreqRowKV(key, val)
			if err != nil {
				return nil, err
			}
			tfrsByDocNum[tfr.DocNum()] = tfr

			// now we have a possible row, but there may be more rows for the same docid
			// find these now
			err = r.findNextTfrsWithSameDocId(tfrsByDocNum, tfr.DocID())
			if err != nil {
				return nil, err
			}

			docNumList := make(DocNumberList, 0, len(tfrsByDocNum))
			for dn := range tfrsByDocNum {
				docNumList = append(docNumList, dn)
			}

			logger.Printf("docNumList: %v", docNumList)

			highestValidDocNum := r.r.s.Which(tfr.docID, docNumList)
			if highestValidDocNum == 0 {
				// no valid doc number
				key, val, valid = r.i.Current()
				continue
			}
			logger.Printf("highest valid: %d", highestValidDocNum)

			tfr = tfrsByDocNum[highestValidDocNum]

			return &index.TermFieldDoc{
				ID:      string(tfr.DocID()),
				Freq:    tfr.Freq(),
				Norm:    float64(tfr.Norm()),
				Vectors: r.termFieldVectorsFromTermVectors(tfr.Vectors()),
			}, nil
		}
	}
	return nil, nil
}

func (r *firestormTermFieldReader) findNextTfrsWithSameDocId(tfrsByDocNum map[uint64]*TermFreqRow, docID []byte) error {
	tfrDocIdPrefix := TermFreqPrefixFieldTermDocId(r.field, r.term, docID)
	r.i.Next()
	key, val, valid := r.i.Current()
	for valid && bytes.HasPrefix(key, tfrDocIdPrefix) {
		tfr, err := NewTermFreqRowKV(key, val)
		if err != nil {
			return err
		}
		tfrsByDocNum[tfr.DocNum()] = tfr
		r.i.Next()
		key, val, valid = r.i.Current()
	}
	return nil
}

func (r *firestormTermFieldReader) Advance(docID string) (*index.TermFieldDoc, error) {
	if r.i != nil {
		tfrDocIdPrefix := TermFreqPrefixFieldTermDocId(r.field, r.term, []byte(docID))
		r.i.Seek(tfrDocIdPrefix)
		return r.Next()
	}
	return nil, nil
}

func (r *firestormTermFieldReader) Count() uint64 {
	return r.count
}

func (r *firestormTermFieldReader) Close() error {
	atomic.AddUint64(&r.r.f.stats.termSearchersFinished, uint64(1))
	if r.i != nil {
		return r.i.Close()
	}
	return nil
}

func (r *firestormTermFieldReader) termFieldVectorsFromTermVectors(in []*TermVector) []*index.TermFieldVector {
	rv := make([]*index.TermFieldVector, len(in))

	for i, tv := range in {
		fieldName := r.r.f.fieldCache.FieldIndexed(uint16(tv.GetField()))
		tfv := index.TermFieldVector{
			Field:          fieldName,
			ArrayPositions: tv.GetArrayPositions(),
			Pos:            tv.GetPos(),
			Start:          tv.GetStart(),
			End:            tv.GetEnd(),
		}
		rv[i] = &tfv
	}
	return rv
}
