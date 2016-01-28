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

	"github.com/blevesearch/bleve/index/store"
)

type firestormDocIDReader struct {
	r     *firestormReader
	start []byte
	i     store.KVIterator
}

func newFirestormDocIDReader(r *firestormReader, start, end string) (*firestormDocIDReader, error) {
	startKey := TermFreqIteratorStart(0, nil)
	if start != "" {
		startKey = TermFreqPrefixFieldTermDocId(0, nil, []byte(start))
	}
	logger.Printf("start key '%s' - % x", startKey, startKey)
	endKey := TermFreqIteratorStart(0, []byte{ByteSeparator})
	if end != "" {
		endKey = TermFreqPrefixFieldTermDocId(0, nil, []byte(end))
	}

	logger.Printf("end key '%s' - % x", endKey, endKey)

	i := r.r.RangeIterator(startKey, endKey)

	rv := firestormDocIDReader{
		r:     r,
		start: startKey,
		i:     i,
	}

	return &rv, nil
}

func (r *firestormDocIDReader) Next() (string, error) {
	if r.i != nil {
		key, val, valid := r.i.Current()
		for valid {
			logger.Printf("see key: '%s' - % x", key, key)
			tfrsByDocNum := make(map[uint64]*TermFreqRow)
			tfr, err := NewTermFreqRowKV(key, val)
			if err != nil {
				return "", err
			}
			tfrsByDocNum[tfr.DocNum()] = tfr

			// now we have a possible row, but there may be more rows for the same docid
			// find these now
			err = r.findNextTfrsWithSameDocId(tfrsByDocNum, tfr.DocID())
			if err != nil {
				return "", err
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
			return string(tfr.DocID()), nil
		}
	}
	return "", nil
}

// FIXME this is identical to the one in reader_terms.go
func (r *firestormDocIDReader) findNextTfrsWithSameDocId(tfrsByDocNum map[uint64]*TermFreqRow, docID []byte) error {
	tfrDocIdPrefix := TermFreqPrefixFieldTermDocId(0, nil, docID)
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

func (r *firestormDocIDReader) Advance(docID string) (string, error) {
	if r.i != nil {
		tfrDocIdPrefix := TermFreqPrefixFieldTermDocId(0, nil, []byte(docID))
		r.i.Seek(tfrDocIdPrefix)
		return r.Next()
	}
	return "", nil
}

func (r *firestormDocIDReader) Close() error {
	if r.i != nil {
		return r.i.Close()
	}
	return nil
}
