//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package smolder

import (
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type SmolderingCouchTermFieldReader struct {
	count       uint64
	indexReader *IndexReader
	iterator    store.KVIterator
	term        []byte
	tfrNext     *TermFrequencyRow
	field       uint16
}

func newSmolderingCouchTermFieldReader(indexReader *IndexReader, term []byte, field uint16, includeFreq, includeNorm, includeTermVectors bool) (*SmolderingCouchTermFieldReader, error) {
	dictionaryRow := NewDictionaryRow(term, field, 0)
	val, err := indexReader.kvreader.Get(dictionaryRow.Key())
	if err != nil {
		return nil, err
	}
	if val == nil {
		atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
		return &SmolderingCouchTermFieldReader{
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

	tfrk := TermFrequencyRowStart(term, field, []byte{})
	it := indexReader.kvreader.PrefixIterator(tfrk)

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
	return &SmolderingCouchTermFieldReader{
		indexReader: indexReader,
		iterator:    it,
		count:       dictionaryRow.count,
		term:        term,
		tfrNext:     &TermFrequencyRow{},
		field:       field,
	}, nil
}

func (r *SmolderingCouchTermFieldReader) Count() uint64 {
	return r.count
}

func (r *SmolderingCouchTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		key, val, valid := r.iterator.Current()
		if valid {
			tfr := r.tfrNext
			err := tfr.parseKDoc(key)
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
			rv.ID = append(rv.ID, tfr.docNumber...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)
			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *SmolderingCouchTermFieldReader) Advance(docID index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		tfrk := TermFrequencyRowStart(r.term, r.field, docID)
		r.iterator.Seek(tfrk)
		key, val, valid := r.iterator.Current()
		if valid {
			tfr, err := NewTermFrequencyRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}
			rv.ID = append(rv.ID, tfr.docNumber...)
			rv.Freq = tfr.freq
			rv.Norm = float64(tfr.norm)
			if tfr.vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(tfr.vectors)
			}
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *SmolderingCouchTermFieldReader) Close() error {
	if r.iterator != nil {
		return r.iterator.Close()
	}
	return nil
}

type SmolderingCouchDocIDReader struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	only        map[string]struct{}
	onlyMode    bool
}

func newSmolderingCouchDocIDReader(indexReader *IndexReader) (*SmolderingCouchDocIDReader, error) {

	startBytes := []byte{0x0}
	endBytes := []byte{0xff}
	bisrk := BackIndexRowKey(startBytes)
	bierk := BackIndexRowKey(endBytes)
	it := indexReader.kvreader.RangeIterator(bisrk, bierk)

	return &SmolderingCouchDocIDReader{
		indexReader: indexReader,
		iterator:    it,
	}, nil
}

func newSmolderingCouchDocIDReaderOnly(indexReader *IndexReader, ids []string) (*SmolderingCouchDocIDReader, error) {
	// put ids into map
	only := make(map[string]struct{}, len(ids))
	for i := range ids {
		only[ids[i]] = struct{}{}
	}

	startBytes := []byte{0x0}
	endBytes := []byte{0xff}
	bisrk := BackIndexRowKey(startBytes)
	bierk := BackIndexRowKey(endBytes)
	it := indexReader.kvreader.RangeIterator(bisrk, bierk)

	return &SmolderingCouchDocIDReader{
		indexReader: indexReader,
		iterator:    it,
		only:        only,
		onlyMode:    true,
	}, nil
}

func (r *SmolderingCouchDocIDReader) Next() (index.IndexInternalID, error) {
	key, val, valid := r.iterator.Current()

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			// find doc id
			for _, te := range br.termsEntries {
				if te.GetField() == 0 {
					for i := range te.Terms {
						if _, ok := r.only[te.Terms[i]]; ok {
							rv = append([]byte(nil), br.docNumber...)
							r.iterator.Next()
							return rv, nil
						}
					}
					break
				}
			}
			r.iterator.Next()
			key, val, valid = r.iterator.Current()
		}
	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.docNumber...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *SmolderingCouchDocIDReader) Advance(docID index.IndexInternalID) (index.IndexInternalID, error) {
	birk := BackIndexRowKey(docID)
	r.iterator.Seek(birk)
	key, val, valid := r.iterator.Current()

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}

			// find doc id
			for _, te := range br.termsEntries {
				if te.GetField() == 0 {
					for i := range te.Terms {
						if _, ok := r.only[te.Terms[i]]; ok {
							rv = append([]byte(nil), br.docNumber...)
							r.iterator.Next()
							return rv, nil
						}
					}
					break
				}
			}
			r.iterator.Next()
			key, val, valid = r.iterator.Current()
		}
	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.docNumber...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *SmolderingCouchDocIDReader) Close() error {
	atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	return r.iterator.Close()
}
