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
	"sort"
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type UpsideDownCouchTermFieldReader struct {
	count       uint64
	indexReader *IndexReader
	iterator    store.KVIterator
	term        []byte
	tfrNext     *TermFrequencyRow
	keyBuf      []byte
	field       uint16
}

func newUpsideDownCouchTermFieldReader(indexReader *IndexReader, term []byte, field uint16, includeFreq, includeNorm, includeTermVectors bool) (*UpsideDownCouchTermFieldReader, error) {
	dictionaryRow := NewDictionaryRow(term, field, 0)
	val, err := indexReader.kvreader.Get(dictionaryRow.Key())
	if err != nil {
		return nil, err
	}
	if val == nil {
		atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
		return &UpsideDownCouchTermFieldReader{
			count:   0,
			term:    term,
			tfrNext: &TermFrequencyRow{},
			field:   field,
		}, nil
	}

	err = dictionaryRow.parseDictionaryV(val)
	if err != nil {
		return nil, err
	}

	tfr := NewTermFrequencyRow(term, field, []byte{}, 0, 0)
	it := indexReader.kvreader.PrefixIterator(tfr.Key())

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
	return &UpsideDownCouchTermFieldReader{
		indexReader: indexReader,
		iterator:    it,
		count:       dictionaryRow.count,
		term:        term,
		field:       field,
	}, nil
}

func (r *UpsideDownCouchTermFieldReader) Count() uint64 {
	return r.count
}

func (r *UpsideDownCouchTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		// We treat tfrNext also like an initialization flag, which
		// tells us whether we need to invoke the underlying
		// iterator.Next().  The first time, don't call iterator.Next().
		if r.tfrNext != nil {
			r.iterator.Next()
		} else {
			r.tfrNext = &TermFrequencyRow{}
		}
		key, val, valid := r.iterator.Current()
		if valid {
			tfr := r.tfrNext
			err := tfr.parseKDoc(key, r.term)
			if err != nil {
				return nil, err
			}
			err = tfr.parseV(val)
			if err != nil {
				return nil, err
			}
			rv := preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}
			rv.ID = append(rv.ID, tfr.doc...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)
			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchTermFieldReader) Advance(docID index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		if r.tfrNext == nil {
			r.tfrNext = &TermFrequencyRow{}
		}
		tfr := InitTermFrequencyRow(r.tfrNext, r.term, r.field, docID, 0, 0)
		keySize := tfr.KeySize()
		if cap(r.keyBuf) < keySize {
			r.keyBuf = make([]byte, keySize)
		}
		keySize, _ = tfr.KeyTo(r.keyBuf[0:keySize])
		r.iterator.Seek(r.keyBuf[0:keySize])
		key, val, valid := r.iterator.Current()
		if valid {
			err := tfr.parseKDoc(key, r.term)
			if err != nil {
				return nil, err
			}
			err = tfr.parseV(val)
			if err != nil {
				return nil, err
			}
			rv := preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}
			rv.ID = append(rv.ID, tfr.doc...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)
			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchTermFieldReader) Close() error {
	if r.iterator != nil {
		return r.iterator.Close()
	}
	return nil
}

type UpsideDownCouchDocIDReader struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	only        []string
	onlyPos     int
	onlyMode    bool
}

func newUpsideDownCouchDocIDReader(indexReader *IndexReader) (*UpsideDownCouchDocIDReader, error) {

	startBytes := []byte{0x0}
	endBytes := []byte{0xff}

	bisr := NewBackIndexRow(startBytes, nil, nil)
	bier := NewBackIndexRow(endBytes, nil, nil)
	it := indexReader.kvreader.RangeIterator(bisr.Key(), bier.Key())

	return &UpsideDownCouchDocIDReader{
		indexReader: indexReader,
		iterator:    it,
	}, nil
}

func newUpsideDownCouchDocIDReaderOnly(indexReader *IndexReader, ids []string) (*UpsideDownCouchDocIDReader, error) {
	// ensure ids are sorted
	sort.Strings(ids)
	startBytes := []byte{0x0}
	if len(ids) > 0 {
		startBytes = []byte(ids[0])
	}
	endBytes := []byte{0xff}
	if len(ids) > 0 {
		endBytes = incrementBytes([]byte(ids[len(ids)-1]))
	}
	bisr := NewBackIndexRow(startBytes, nil, nil)
	bier := NewBackIndexRow(endBytes, nil, nil)
	it := indexReader.kvreader.RangeIterator(bisr.Key(), bier.Key())

	return &UpsideDownCouchDocIDReader{
		indexReader: indexReader,
		iterator:    it,
		only:        ids,
		onlyMode:    true,
	}, nil
}

func (r *UpsideDownCouchDocIDReader) Next() (index.IndexInternalID, error) {
	key, val, valid := r.iterator.Current()

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				ok := r.nextOnly()
				if !ok {
					return nil, nil
				}
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()
				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}
		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}
			return rv, nil
		}

	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchDocIDReader) Advance(docID index.IndexInternalID) (index.IndexInternalID, error) {
	bir := NewBackIndexRow(docID, nil, nil)
	r.iterator.Seek(bir.Key())
	key, val, valid := r.iterator.Current()
	r.onlyPos = sort.SearchStrings(r.only, string(docID))

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				ok := r.nextOnly()
				if !ok {
					return nil, nil
				}
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}
		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}
			return rv, nil
		}
	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchDocIDReader) Close() error {
	atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	return r.iterator.Close()
}

// move the r.only pos forward one, skipping duplicates
// return true if there is more data, or false if we got to the end of the list
func (r *UpsideDownCouchDocIDReader) nextOnly() bool {

	// advance 1 position, until we see a different key
	//   it's already sorted, so this skips duplicates
	start := r.onlyPos
	r.onlyPos++
	for r.onlyPos < len(r.only) && r.only[r.onlyPos] == r.only[start] {
		start = r.onlyPos
		r.onlyPos++
	}
	// inidicate if we got to the end of the list
	return r.onlyPos < len(r.only)
}
