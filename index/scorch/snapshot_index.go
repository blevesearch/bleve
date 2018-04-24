//  Copyright (c) 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorch

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type asynchSegmentResult struct {
	dictItr segment.DictionaryIterator

	index int
	docs  *roaring.Bitmap

	postings segment.PostingsList

	err error
}

var reflectStaticSizeIndexSnapshot int

func init() {
	var is interface{} = IndexSnapshot{}
	reflectStaticSizeIndexSnapshot = int(reflect.TypeOf(is).Size())
}

type IndexSnapshot struct {
	parent   *Scorch
	segment  []*SegmentSnapshot
	offsets  []uint64
	internal map[string][]byte
	epoch    uint64
	size     uint64
	creator  string

	m    sync.Mutex // Protects the fields that follow.
	refs int64

	m2         sync.Mutex                                 // Protects the fields that follow.
	fieldTFRs  map[string][]*IndexSnapshotTermFieldReader // keyed by field, recycled TFR's
	fieldDicts map[string][]segment.TermDictionary        // keyed by field, recycled dicts
}

func (i *IndexSnapshot) Segments() []*SegmentSnapshot {
	return i.segment
}

func (i *IndexSnapshot) Internal() map[string][]byte {
	return i.internal
}

func (i *IndexSnapshot) AddRef() {
	i.m.Lock()
	i.refs++
	i.m.Unlock()
}

func (i *IndexSnapshot) DecRef() (err error) {
	i.m.Lock()
	i.refs--
	if i.refs == 0 {
		for _, s := range i.segment {
			if s != nil {
				err2 := s.segment.DecRef()
				if err == nil {
					err = err2
				}
			}
		}
		if i.parent != nil {
			go i.parent.AddEligibleForRemoval(i.epoch)
		}
	}
	i.m.Unlock()
	return err
}

func (i *IndexSnapshot) Close() error {
	return i.DecRef()
}

func (i *IndexSnapshot) Size() int {
	return int(i.size)
}

func (i *IndexSnapshot) updateSize() {
	i.size += uint64(reflectStaticSizeIndexSnapshot)
	for _, s := range i.segment {
		i.size += uint64(s.Size())
	}
}

func (i *IndexSnapshot) newIndexSnapshotFieldDict(field string, makeItr func(i segment.TermDictionary) segment.DictionaryIterator) (*IndexSnapshotFieldDict, error) {

	results := make(chan *asynchSegmentResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			dict, err := segment.Dictionary(field)
			if err != nil {
				results <- &asynchSegmentResult{err: err}
			} else {
				results <- &asynchSegmentResult{dictItr: makeItr(dict)}
			}
		}(index, segment)
	}

	var err error
	rv := &IndexSnapshotFieldDict{
		snapshot: i,
		cursors:  make([]*segmentDictCursor, 0, len(i.segment)),
	}
	for count := 0; count < len(i.segment); count++ {
		asr := <-results
		if asr.err != nil && err == nil {
			err = asr.err
		} else {
			next, err2 := asr.dictItr.Next()
			if err2 != nil && err == nil {
				err = err2
			}
			if next != nil {
				rv.cursors = append(rv.cursors, &segmentDictCursor{
					itr:  asr.dictItr,
					curr: *next,
				})
			}
		}
	}
	// after ensuring we've read all items on channel
	if err != nil {
		return nil, err
	}
	// prepare heap
	heap.Init(rv)

	return rv, nil
}

func (i *IndexSnapshot) FieldDict(field string) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.Iterator()
	})
}

func (i *IndexSnapshot) FieldDictRange(field string, startTerm []byte,
	endTerm []byte) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.RangeIterator(string(startTerm), string(endTerm))
	})
}

func (i *IndexSnapshot) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.PrefixIterator(string(termPrefix))
	})
}

func (i *IndexSnapshot) FieldDictRegexp(field string,
	termRegex []byte) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.RegexpIterator(string(termRegex))
	})
}

func (i *IndexSnapshot) FieldDictFuzzy(field string,
	term []byte, fuzziness int) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.FuzzyIterator(string(term), fuzziness)
	})
}

func (i *IndexSnapshot) FieldDictOnly(field string,
	onlyTerms [][]byte, includeCount bool) (index.FieldDict, error) {
	return i.newIndexSnapshotFieldDict(field, func(i segment.TermDictionary) segment.DictionaryIterator {
		return i.OnlyIterator(onlyTerms, includeCount)
	})
}

func (i *IndexSnapshot) DocIDReaderAll() (index.DocIDReader, error) {
	results := make(chan *asynchSegmentResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			results <- &asynchSegmentResult{
				index: index,
				docs:  segment.DocNumbersLive(),
			}
		}(index, segment)
	}

	return i.newDocIDReader(results)
}

func (i *IndexSnapshot) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	results := make(chan *asynchSegmentResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			docs, err := segment.DocNumbers(ids)
			if err != nil {
				results <- &asynchSegmentResult{err: err}
			} else {
				results <- &asynchSegmentResult{
					index: index,
					docs:  docs,
				}
			}
		}(index, segment)
	}

	return i.newDocIDReader(results)
}

func (i *IndexSnapshot) newDocIDReader(results chan *asynchSegmentResult) (index.DocIDReader, error) {
	rv := &IndexSnapshotDocIDReader{
		snapshot:  i,
		iterators: make([]roaring.IntIterable, len(i.segment)),
	}
	var err error
	for count := 0; count < len(i.segment); count++ {
		asr := <-results
		if asr.err != nil && err != nil {
			err = asr.err
		} else {
			rv.iterators[asr.index] = asr.docs.Iterator()
		}
	}

	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (i *IndexSnapshot) Fields() ([]string, error) {
	// FIXME not making this concurrent for now as it's not used in hot path
	// of any searches at the moment (just a debug aid)
	fieldsMap := map[string]struct{}{}
	for _, segment := range i.segment {
		fields := segment.Fields()
		for _, field := range fields {
			fieldsMap[field] = struct{}{}
		}
	}
	rv := make([]string, 0, len(fieldsMap))
	for k := range fieldsMap {
		rv = append(rv, k)
	}
	return rv, nil
}

func (i *IndexSnapshot) GetInternal(key []byte) ([]byte, error) {
	return i.internal[string(key)], nil
}

func (i *IndexSnapshot) DocCount() (uint64, error) {
	var rv uint64
	for _, segment := range i.segment {
		rv += segment.Count()
	}
	return rv, nil
}

func (i *IndexSnapshot) Document(id string) (rv *document.Document, err error) {
	// FIXME could be done more efficiently directly, but reusing for simplicity
	tfr, err := i.TermFieldReader([]byte(id), "_id", false, false, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := tfr.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	next, err := tfr.Next(nil)
	if err != nil {
		return nil, err
	}

	if next == nil {
		// no such doc exists
		return nil, nil
	}

	docNum, err := docInternalToNumber(next.ID)
	if err != nil {
		return nil, err
	}
	segmentIndex, localDocNum := i.segmentIndexAndLocalDocNumFromGlobal(docNum)

	rv = document.NewDocument(id)
	err = i.segment[segmentIndex].VisitDocument(localDocNum, func(name string, typ byte, value []byte, pos []uint64) bool {
		if name == "_id" {
			return true
		}
		switch typ {
		case 't':
			rv.AddField(document.NewTextField(name, pos, value))
		case 'n':
			rv.AddField(document.NewNumericFieldFromBytes(name, pos, value))
		case 'd':
			rv.AddField(document.NewDateTimeFieldFromBytes(name, pos, value))
		case 'b':
			rv.AddField(document.NewBooleanFieldFromBytes(name, pos, value))
		case 'g':
			rv.AddField(document.NewGeoPointFieldFromBytes(name, pos, value))
		}

		return true
	})
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (i *IndexSnapshot) segmentIndexAndLocalDocNumFromGlobal(docNum uint64) (int, uint64) {
	segmentIndex := sort.Search(len(i.offsets),
		func(x int) bool {
			return i.offsets[x] > docNum
		}) - 1

	localDocNum := docNum - i.offsets[segmentIndex]
	return int(segmentIndex), localDocNum
}

func (i *IndexSnapshot) ExternalID(id index.IndexInternalID) (string, error) {
	docNum, err := docInternalToNumber(id)
	if err != nil {
		return "", err
	}
	segmentIndex, localDocNum := i.segmentIndexAndLocalDocNumFromGlobal(docNum)

	v, err := i.segment[segmentIndex].DocID(localDocNum)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", fmt.Errorf("document number %d not found", docNum)
	}

	return string(v), nil
}

func (i *IndexSnapshot) InternalID(id string) (rv index.IndexInternalID, err error) {
	// FIXME could be done more efficiently directly, but reusing for simplicity
	tfr, err := i.TermFieldReader([]byte(id), "_id", false, false, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := tfr.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	next, err := tfr.Next(nil)
	if err != nil || next == nil {
		return nil, err
	}

	return next.ID, nil
}

func (i *IndexSnapshot) TermFieldReader(term []byte, field string, includeFreq,
	includeNorm, includeTermVectors bool) (tfr index.TermFieldReader, err error) {
	rv, dicts := i.allocTermFieldReaderDicts(field)

	rv.term = term
	rv.field = field
	rv.snapshot = i
	if rv.postings == nil {
		rv.postings = make([]segment.PostingsList, len(i.segment))
	}
	if rv.iterators == nil {
		rv.iterators = make([]segment.PostingsIterator, len(i.segment))
	}
	rv.segmentOffset = 0
	rv.includeFreq = includeFreq
	rv.includeNorm = includeNorm
	rv.includeTermVectors = includeTermVectors
	rv.currPosting = nil
	rv.currID = rv.currID[:0]

	if dicts == nil {
		dicts = make([]segment.TermDictionary, len(i.segment))
		for i, segment := range i.segment {
			dict, err := segment.Dictionary(field)
			if err != nil {
				return nil, err
			}
			dicts[i] = dict
		}
	}
	rv.dicts = dicts

	for i := range i.segment {
		pl, err := dicts[i].PostingsList(term, nil, rv.postings[i])
		if err != nil {
			return nil, err
		}
		rv.postings[i] = pl
		rv.iterators[i] = pl.Iterator(includeFreq, includeNorm, includeTermVectors, rv.iterators[i])
	}
	atomic.AddUint64(&i.parent.stats.TotTermSearchersStarted, uint64(1))
	return rv, nil
}

func (i *IndexSnapshot) allocTermFieldReaderDicts(field string) (
	tfr *IndexSnapshotTermFieldReader, dicts []segment.TermDictionary) {
	i.m2.Lock()
	if i.fieldDicts != nil {
		dicts = i.fieldDicts[field]
	}
	if i.fieldTFRs != nil {
		tfrs := i.fieldTFRs[field]
		last := len(tfrs) - 1
		if last >= 0 {
			rv := tfrs[last]
			tfrs[last] = nil
			i.fieldTFRs[field] = tfrs[:last]
			i.m2.Unlock()
			return rv, dicts
		}
	}
	i.m2.Unlock()
	return &IndexSnapshotTermFieldReader{}, dicts
}

func (i *IndexSnapshot) recycleTermFieldReader(tfr *IndexSnapshotTermFieldReader) {
	i.m2.Lock()
	if i.fieldTFRs == nil {
		i.fieldTFRs = map[string][]*IndexSnapshotTermFieldReader{}
	}
	i.fieldTFRs[tfr.field] = append(i.fieldTFRs[tfr.field], tfr)
	if i.fieldDicts == nil {
		i.fieldDicts = map[string][]segment.TermDictionary{}
	}
	i.fieldDicts[tfr.field] = tfr.dicts
	i.m2.Unlock()
}

func docNumberToBytes(buf []byte, in uint64) []byte {
	if len(buf) != 8 {
		if cap(buf) >= 8 {
			buf = buf[0:8]
		} else {
			buf = make([]byte, 8)
		}
	}
	binary.BigEndian.PutUint64(buf, in)
	return buf
}

func docInternalToNumber(in index.IndexInternalID) (uint64, error) {
	if len(in) != 8 {
		return 0, fmt.Errorf("wrong len for IndexInternalID: %q", in)
	}
	return binary.BigEndian.Uint64(in), nil
}

func (i *IndexSnapshot) DocumentVisitFieldTerms(id index.IndexInternalID,
	fields []string, visitor index.DocumentFieldTermVisitor) error {

	docNum, err := docInternalToNumber(id)
	if err != nil {
		return err
	}
	segmentIndex, localDocNum := i.segmentIndexAndLocalDocNumFromGlobal(docNum)
	if segmentIndex >= len(i.segment) {
		return nil
	}

	ss := i.segment[segmentIndex]

	if zaps, ok := ss.segment.(segment.DocumentFieldTermVisitable); ok {
		// get the list of doc value persisted fields
		pFields, err := zaps.VisitableDocValueFields()
		if err != nil {
			return err
		}
		// assort the fields for which terms look up have to
		// be performed runtime
		dvPendingFields := extractDvPendingFields(fields, pFields)
		if len(dvPendingFields) == 0 {
			// all fields are doc value persisted
			return zaps.VisitDocumentFieldTerms(localDocNum, fields, visitor)
		}

		// concurrently trigger the runtime doc value preparations for
		// pending fields as well as the visit of the persisted doc values
		errCh := make(chan error, 1)

		go func() {
			defer close(errCh)
			err := ss.cachedDocs.prepareFields(fields, ss)
			if err != nil {
				errCh <- err
			}
		}()

		// visit the persisted dv while the cache preparation is in progress
		err = zaps.VisitDocumentFieldTerms(localDocNum, fields, visitor)
		if err != nil {
			return err
		}

		// err out if fieldCache preparation failed
		err = <-errCh
		if err != nil {
			return err
		}

		visitDocumentFieldCacheTerms(localDocNum, dvPendingFields, ss, visitor)
		return nil
	}

	return prepareCacheVisitDocumentFieldTerms(localDocNum, fields, ss, visitor)
}

func prepareCacheVisitDocumentFieldTerms(localDocNum uint64, fields []string,
	ss *SegmentSnapshot, visitor index.DocumentFieldTermVisitor) error {
	err := ss.cachedDocs.prepareFields(fields, ss)
	if err != nil {
		return err
	}

	visitDocumentFieldCacheTerms(localDocNum, fields, ss, visitor)
	return nil
}

func visitDocumentFieldCacheTerms(localDocNum uint64, fields []string,
	ss *SegmentSnapshot, visitor index.DocumentFieldTermVisitor) {

	for _, field := range fields {
		if cachedFieldDocs, exists := ss.cachedDocs.cache[field]; exists {
			if tlist, exists := cachedFieldDocs.docs[localDocNum]; exists {
				for {
					i := bytes.Index(tlist, TermSeparatorSplitSlice)
					if i < 0 {
						break
					}
					visitor(field, tlist[0:i])
					tlist = tlist[i+1:]
				}
			}
		}
	}

}

func extractDvPendingFields(requestedFields, persistedFields []string) []string {
	removeMap := make(map[string]struct{}, len(persistedFields))
	for _, str := range persistedFields {
		removeMap[str] = struct{}{}
	}

	rv := make([]string, 0, len(requestedFields))
	for _, s := range requestedFields {
		if _, ok := removeMap[s]; !ok {
			rv = append(rv, s)
		}
	}
	return rv
}

func (i *IndexSnapshot) DumpAll() chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}

func (i *IndexSnapshot) DumpDoc(id string) chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}

func (i *IndexSnapshot) DumpFields() chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}
