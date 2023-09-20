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
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
	lev "github.com/blevesearch/vellum/levenshtein"
	bolt "go.etcd.io/bbolt"
)

// re usable, threadsafe levenshtein builders
var lb1, lb2 *lev.LevenshteinAutomatonBuilder

type asynchSegmentResult struct {
	dict    segment.TermDictionary
	dictItr segment.DictionaryIterator

	index int
	docs  *roaring.Bitmap

	postings segment.PostingsList

	err error
}

var reflectStaticSizeIndexSnapshot int

// DefaultFieldTFRCacheThreshold limits the number of TermFieldReaders(TFR) for
// a field in an index snapshot. Without this limit, when recycling TFRs, it is
// possible that a very large number of TFRs may be added to the recycle
// cache, which could eventually lead to significant memory consumption.
// This threshold can be overwritten by users at the library level by changing the
// exported variable, or at the index level by setting the FieldTFRCacheThreshold
// in the kvConfig.
var DefaultFieldTFRCacheThreshold uint64 = 10

func init() {
	var is interface{} = IndexSnapshot{}
	reflectStaticSizeIndexSnapshot = int(reflect.TypeOf(is).Size())
	var err error
	lb1, err = lev.NewLevenshteinAutomatonBuilder(1, true)
	if err != nil {
		panic(fmt.Errorf("Levenshtein automaton ed1 builder err: %v", err))
	}
	lb2, err = lev.NewLevenshteinAutomatonBuilder(2, true)
	if err != nil {
		panic(fmt.Errorf("Levenshtein automaton ed2 builder err: %v", err))
	}
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

	m2        sync.Mutex                                 // Protects the fields that follow.
	fieldTFRs map[string][]*IndexSnapshotTermFieldReader // keyed by field, recycled TFR's
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

func (is *IndexSnapshot) newIndexSnapshotFieldDict(field string,
	makeItr func(i segment.TermDictionary) segment.DictionaryIterator,
	randomLookup bool) (*IndexSnapshotFieldDict, error) {

	results := make(chan *asynchSegmentResult)
	var totalBytesRead uint64
	for _, s := range is.segment {
		go func(s *SegmentSnapshot) {
			dict, err := s.segment.Dictionary(field)
			if err != nil {
				results <- &asynchSegmentResult{err: err}
			} else {
				if dictStats, ok := dict.(segment.DiskStatsReporter); ok {
					atomic.AddUint64(&totalBytesRead, dictStats.BytesRead())
				}
				if randomLookup {
					results <- &asynchSegmentResult{dict: dict}
				} else {
					results <- &asynchSegmentResult{dictItr: makeItr(dict)}
				}
			}
		}(s)
	}

	var err error
	rv := &IndexSnapshotFieldDict{
		snapshot: is,
		cursors:  make([]*segmentDictCursor, 0, len(is.segment)),
	}
	for count := 0; count < len(is.segment); count++ {
		asr := <-results
		if asr.err != nil && err == nil {
			err = asr.err
		} else {
			if !randomLookup {
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
			} else {
				rv.cursors = append(rv.cursors, &segmentDictCursor{
					dict: asr.dict,
				})
			}
		}
	}
	rv.bytesRead = totalBytesRead
	// after ensuring we've read all items on channel
	if err != nil {
		return nil, err
	}

	if !randomLookup {
		// prepare heap
		heap.Init(rv)
	}

	return rv, nil
}

func (is *IndexSnapshot) FieldDict(field string) (index.FieldDict, error) {
	return is.newIndexSnapshotFieldDict(field, func(is segment.TermDictionary) segment.DictionaryIterator {
		return is.AutomatonIterator(nil, nil, nil)
	}, false)
}

// calculateExclusiveEndFromInclusiveEnd produces the next key
// when sorting using memcmp style comparisons, suitable to
// use as the end key in a traditional (inclusive, exclusive]
// start/end range
func calculateExclusiveEndFromInclusiveEnd(inclusiveEnd []byte) []byte {
	rv := inclusiveEnd
	if len(inclusiveEnd) > 0 {
		rv = make([]byte, len(inclusiveEnd))
		copy(rv, inclusiveEnd)
		if rv[len(rv)-1] < 0xff {
			// last byte can be incremented by one
			rv[len(rv)-1]++
		} else {
			// last byte is already 0xff, so append 0
			// next key is simply one byte longer
			rv = append(rv, 0x0)
		}
	}
	return rv
}

func (is *IndexSnapshot) FieldDictRange(field string, startTerm []byte,
	endTerm []byte) (index.FieldDict, error) {
	return is.newIndexSnapshotFieldDict(field, func(is segment.TermDictionary) segment.DictionaryIterator {
		endTermExclusive := calculateExclusiveEndFromInclusiveEnd(endTerm)
		return is.AutomatonIterator(nil, startTerm, endTermExclusive)
	}, false)
}

// calculateExclusiveEndFromPrefix produces the first key that
// does not have the same prefix as the input bytes, suitable
// to use as the end key in a traditional (inclusive, exclusive]
// start/end range
func calculateExclusiveEndFromPrefix(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			return rv // didn't overflow, so stop
		}
	}
	// all bytes were 0xff, so return nil
	// as there is no end key for this prefix
	return nil
}

func (is *IndexSnapshot) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	termPrefixEnd := calculateExclusiveEndFromPrefix(termPrefix)
	return is.newIndexSnapshotFieldDict(field, func(is segment.TermDictionary) segment.DictionaryIterator {
		return is.AutomatonIterator(nil, termPrefix, termPrefixEnd)
	}, false)
}

func (is *IndexSnapshot) FieldDictRegexp(field string,
	termRegex string) (index.FieldDict, error) {
	// TODO: potential optimization where the literal prefix represents the,
	//       entire regexp, allowing us to use PrefixIterator(prefixTerm)?

	a, prefixBeg, prefixEnd, err := parseRegexp(termRegex)
	if err != nil {
		return nil, err
	}

	return is.newIndexSnapshotFieldDict(field, func(is segment.TermDictionary) segment.DictionaryIterator {
		return is.AutomatonIterator(a, prefixBeg, prefixEnd)
	}, false)
}

func (is *IndexSnapshot) getLevAutomaton(term string,
	fuzziness uint8) (vellum.Automaton, error) {
	if fuzziness == 1 {
		return lb1.BuildDfa(term, fuzziness)
	} else if fuzziness == 2 {
		return lb2.BuildDfa(term, fuzziness)
	}
	return nil, fmt.Errorf("fuzziness exceeds the max limit")
}

func (is *IndexSnapshot) FieldDictFuzzy(field string,
	term string, fuzziness int, prefix string) (index.FieldDict, error) {
	a, err := is.getLevAutomaton(term, uint8(fuzziness))
	if err != nil {
		return nil, err
	}

	var prefixBeg, prefixEnd []byte
	if prefix != "" {
		prefixBeg = []byte(prefix)
		prefixEnd = calculateExclusiveEndFromPrefix(prefixBeg)
	}

	return is.newIndexSnapshotFieldDict(field, func(is segment.TermDictionary) segment.DictionaryIterator {
		return is.AutomatonIterator(a, prefixBeg, prefixEnd)
	}, false)
}

func (is *IndexSnapshot) FieldDictContains(field string) (index.FieldDictContains, error) {
	return is.newIndexSnapshotFieldDict(field, nil, true)
}

func (is *IndexSnapshot) DocIDReaderAll() (index.DocIDReader, error) {
	results := make(chan *asynchSegmentResult)
	for index, segment := range is.segment {
		go func(index int, segment *SegmentSnapshot) {
			results <- &asynchSegmentResult{
				index: index,
				docs:  segment.DocNumbersLive(),
			}
		}(index, segment)
	}

	return is.newDocIDReader(results)
}

func (is *IndexSnapshot) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	results := make(chan *asynchSegmentResult)
	for index, segment := range is.segment {
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

	return is.newDocIDReader(results)
}

func (is *IndexSnapshot) newDocIDReader(results chan *asynchSegmentResult) (index.DocIDReader, error) {
	rv := &IndexSnapshotDocIDReader{
		snapshot:  is,
		iterators: make([]roaring.IntIterable, len(is.segment)),
	}
	var err error
	for count := 0; count < len(is.segment); count++ {
		asr := <-results
		if asr.err != nil {
			if err == nil {
				// returns the first error encountered
				err = asr.err
			}
		} else if err == nil {
			rv.iterators[asr.index] = asr.docs.Iterator()
		}
	}

	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (is *IndexSnapshot) Fields() ([]string, error) {
	// FIXME not making this concurrent for now as it's not used in hot path
	// of any searches at the moment (just a debug aid)
	fieldsMap := map[string]struct{}{}
	for _, segment := range is.segment {
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

func (is *IndexSnapshot) GetInternal(key []byte) ([]byte, error) {
	return is.internal[string(key)], nil
}

func (is *IndexSnapshot) DocCount() (uint64, error) {
	var rv uint64
	for _, segment := range is.segment {
		rv += segment.Count()
	}
	return rv, nil
}

func (is *IndexSnapshot) Document(id string) (rv index.Document, err error) {
	// FIXME could be done more efficiently directly, but reusing for simplicity
	tfr, err := is.TermFieldReader(nil, []byte(id), "_id", false, false, false)
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
	segmentIndex, localDocNum := is.segmentIndexAndLocalDocNumFromGlobal(docNum)

	rvd := document.NewDocument(id)

	err = is.segment[segmentIndex].VisitDocument(localDocNum, func(name string, typ byte, val []byte, pos []uint64) bool {
		if name == "_id" {
			return true
		}

		// track uncompressed stored fields bytes as part of IO stats.
		// However, ideally we'd need to track the compressed on-disk value
		// Keeping that TODO for now until we have a cleaner way.
		rvd.StoredFieldsSize += uint64(len(val))

		// copy value, array positions to preserve them beyond the scope of this callback
		value := append([]byte(nil), val...)
		arrayPos := append([]uint64(nil), pos...)

		switch typ {
		case 't':
			rvd.AddField(document.NewTextField(name, arrayPos, value))
		case 'n':
			rvd.AddField(document.NewNumericFieldFromBytes(name, arrayPos, value))
		case 'i':
			rvd.AddField(document.NewIPFieldFromBytes(name, arrayPos, value))
		case 'd':
			rvd.AddField(document.NewDateTimeFieldFromBytes(name, arrayPos, value))
		case 'b':
			rvd.AddField(document.NewBooleanFieldFromBytes(name, arrayPos, value))
		case 'g':
			rvd.AddField(document.NewGeoPointFieldFromBytes(name, arrayPos, value))
		case 's':
			rvd.AddField(document.NewGeoShapeFieldFromBytes(name, arrayPos, value))
		}

		return true
	})
	if err != nil {
		return nil, err
	}

	return rvd, nil
}

func (is *IndexSnapshot) segmentIndexAndLocalDocNumFromGlobal(docNum uint64) (int, uint64) {
	segmentIndex := sort.Search(len(is.offsets),
		func(x int) bool {
			return is.offsets[x] > docNum
		}) - 1

	localDocNum := docNum - is.offsets[segmentIndex]
	return int(segmentIndex), localDocNum
}

func (is *IndexSnapshot) ExternalID(id index.IndexInternalID) (string, error) {
	docNum, err := docInternalToNumber(id)
	if err != nil {
		return "", err
	}
	segmentIndex, localDocNum := is.segmentIndexAndLocalDocNumFromGlobal(docNum)

	v, err := is.segment[segmentIndex].DocID(localDocNum)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", fmt.Errorf("document number %d not found", docNum)
	}

	return string(v), nil
}

func (is *IndexSnapshot) InternalID(id string) (rv index.IndexInternalID, err error) {
	// FIXME could be done more efficiently directly, but reusing for simplicity
	tfr, err := is.TermFieldReader(nil, []byte(id), "_id", false, false, false)
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

func (is *IndexSnapshot) TermFieldReader(ctx context.Context, term []byte, field string, includeFreq,
	includeNorm, includeTermVectors bool) (index.TermFieldReader, error) {
	rv := is.allocTermFieldReaderDicts(field)

	rv.ctx = ctx
	rv.term = term
	rv.field = field
	rv.snapshot = is
	if rv.postings == nil {
		rv.postings = make([]segment.PostingsList, len(is.segment))
	}
	if rv.iterators == nil {
		rv.iterators = make([]segment.PostingsIterator, len(is.segment))
	}
	rv.segmentOffset = 0
	rv.includeFreq = includeFreq
	rv.includeNorm = includeNorm
	rv.includeTermVectors = includeTermVectors
	rv.currPosting = nil
	rv.currID = rv.currID[:0]

	if rv.dicts == nil {
		rv.dicts = make([]segment.TermDictionary, len(is.segment))
		for i, s := range is.segment {
			// the intention behind this compare and swap operation is
			// to make sure that the accounting of the metadata is happening
			// only once(which corresponds to this persisted segment's most
			// recent segPlugin.Open() call), and any subsequent queries won't
			// incur this cost which would essentially be a double counting.
			if atomic.CompareAndSwapUint32(&s.mmaped, 1, 0) {
				segBytesRead := s.segment.BytesRead()
				rv.incrementBytesRead(segBytesRead)
			}
			dict, err := s.segment.Dictionary(field)
			if err != nil {
				return nil, err
			}
			if dictStats, ok := dict.(segment.DiskStatsReporter); ok {
				bytesRead := dictStats.BytesRead()
				rv.incrementBytesRead(bytesRead)
			}
			rv.dicts[i] = dict
		}
	}

	for i, s := range is.segment {
		var prevBytesReadPL uint64
		if rv.postings[i] != nil {
			prevBytesReadPL = rv.postings[i].BytesRead()
		}
		pl, err := rv.dicts[i].PostingsList(term, s.deleted, rv.postings[i])
		if err != nil {
			return nil, err
		}
		rv.postings[i] = pl

		var prevBytesReadItr uint64
		if rv.iterators[i] != nil {
			prevBytesReadItr = rv.iterators[i].BytesRead()
		}
		rv.iterators[i] = pl.Iterator(includeFreq, includeNorm, includeTermVectors, rv.iterators[i])

		if bytesRead := rv.postings[i].BytesRead(); prevBytesReadPL < bytesRead {
			rv.incrementBytesRead(bytesRead - prevBytesReadPL)
		}

		if bytesRead := rv.iterators[i].BytesRead(); prevBytesReadItr < bytesRead {
			rv.incrementBytesRead(bytesRead - prevBytesReadItr)
		}
	}
	atomic.AddUint64(&is.parent.stats.TotTermSearchersStarted, uint64(1))
	return rv, nil
}

func (is *IndexSnapshot) allocTermFieldReaderDicts(field string) (tfr *IndexSnapshotTermFieldReader) {
	is.m2.Lock()
	if is.fieldTFRs != nil {
		tfrs := is.fieldTFRs[field]
		last := len(tfrs) - 1
		if last >= 0 {
			tfr = tfrs[last]
			tfrs[last] = nil
			is.fieldTFRs[field] = tfrs[:last]
			is.m2.Unlock()
			return
		}
	}
	is.m2.Unlock()
	return &IndexSnapshotTermFieldReader{
		recycle: true,
	}
}

func (is *IndexSnapshot) getFieldTFRCacheThreshold() uint64 {
	if is.parent.config != nil {
		if _, ok := is.parent.config["FieldTFRCacheThreshold"]; ok {
			return is.parent.config["FieldTFRCacheThreshold"].(uint64)
		}
	}
	return DefaultFieldTFRCacheThreshold
}

func (is *IndexSnapshot) recycleTermFieldReader(tfr *IndexSnapshotTermFieldReader) {
	if !tfr.recycle {
		// Do not recycle an optimized unadorned term field reader (used for
		// ConjunctionUnadorned or DisjunctionUnadorned), during when a fresh
		// roaring.Bitmap is built by AND-ing or OR-ing individual bitmaps,
		// and we'll need to release them for GC. (See MB-40916)
		return
	}

	is.parent.rootLock.RLock()
	obsolete := is.parent.root != is
	is.parent.rootLock.RUnlock()
	if obsolete {
		// if we're not the current root (mutations happened), don't bother recycling
		return
	}

	is.m2.Lock()
	if is.fieldTFRs == nil {
		is.fieldTFRs = map[string][]*IndexSnapshotTermFieldReader{}
	}
	if uint64(len(is.fieldTFRs[tfr.field])) < is.getFieldTFRCacheThreshold() {
		tfr.bytesRead = 0
		is.fieldTFRs[tfr.field] = append(is.fieldTFRs[tfr.field], tfr)
	}
	is.m2.Unlock()
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

func (is *IndexSnapshot) documentVisitFieldTermsOnSegment(
	segmentIndex int, localDocNum uint64, fields []string, cFields []string,
	visitor index.DocValueVisitor, dvs segment.DocVisitState) (
	cFieldsOut []string, dvsOut segment.DocVisitState, err error) {
	ss := is.segment[segmentIndex]

	var vFields []string // fields that are visitable via the segment

	ssv, ssvOk := ss.segment.(segment.DocValueVisitable)
	if ssvOk && ssv != nil {
		vFields, err = ssv.VisitableDocValueFields()
		if err != nil {
			return nil, nil, err
		}
	}

	var errCh chan error

	// cFields represents the fields that we'll need from the
	// cachedDocs, and might be optionally be provided by the caller,
	// if the caller happens to know we're on the same segmentIndex
	// from a previous invocation
	if cFields == nil {
		cFields = subtractStrings(fields, vFields)

		if !ss.cachedDocs.hasFields(cFields) {
			errCh = make(chan error, 1)

			go func() {
				err := ss.cachedDocs.prepareFields(cFields, ss)
				if err != nil {
					errCh <- err
				}
				close(errCh)
			}()
		}
	}

	if ssvOk && ssv != nil && len(vFields) > 0 {
		dvs, err = ssv.VisitDocValues(localDocNum, fields, visitor, dvs)
		if err != nil {
			return nil, nil, err
		}
	}

	if errCh != nil {
		err = <-errCh
		if err != nil {
			return nil, nil, err
		}
	}

	if len(cFields) > 0 {
		ss.cachedDocs.visitDoc(localDocNum, cFields, visitor)
	}

	return cFields, dvs, nil
}

func (is *IndexSnapshot) DocValueReader(fields []string) (
	index.DocValueReader, error) {
	return &DocValueReader{i: is, fields: fields, currSegmentIndex: -1}, nil
}

type DocValueReader struct {
	i      *IndexSnapshot
	fields []string
	dvs    segment.DocVisitState

	currSegmentIndex int
	currCachedFields []string

	totalBytesRead uint64
	bytesRead      uint64
}

func (dvr *DocValueReader) BytesRead() uint64 {
	return dvr.totalBytesRead + dvr.bytesRead
}

func (dvr *DocValueReader) VisitDocValues(id index.IndexInternalID,
	visitor index.DocValueVisitor) (err error) {
	docNum, err := docInternalToNumber(id)
	if err != nil {
		return err
	}

	segmentIndex, localDocNum := dvr.i.segmentIndexAndLocalDocNumFromGlobal(docNum)
	if segmentIndex >= len(dvr.i.segment) {
		return nil
	}

	if dvr.currSegmentIndex != segmentIndex {
		dvr.currSegmentIndex = segmentIndex
		dvr.currCachedFields = nil
		dvr.totalBytesRead += dvr.bytesRead
		dvr.bytesRead = 0
	}

	dvr.currCachedFields, dvr.dvs, err = dvr.i.documentVisitFieldTermsOnSegment(
		dvr.currSegmentIndex, localDocNum, dvr.fields, dvr.currCachedFields, visitor, dvr.dvs)

	if dvr.dvs != nil {
		dvr.bytesRead = dvr.dvs.BytesRead()
	}
	return err
}

func (is *IndexSnapshot) DumpAll() chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}

func (is *IndexSnapshot) DumpDoc(id string) chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}

func (is *IndexSnapshot) DumpFields() chan interface{} {
	rv := make(chan interface{})
	go func() {
		close(rv)
	}()
	return rv
}

func (is *IndexSnapshot) diskSegmentsPaths() map[string]struct{} {
	rv := make(map[string]struct{}, len(is.segment))
	for _, s := range is.segment {
		if seg, ok := s.segment.(segment.PersistedSegment); ok {
			rv[seg.Path()] = struct{}{}
		}
	}
	return rv
}

// reClaimableDocsRatio gives a ratio about the obsoleted or
// reclaimable documents present in a given index snapshot.
func (is *IndexSnapshot) reClaimableDocsRatio() float64 {
	var totalCount, liveCount uint64
	for _, s := range is.segment {
		if _, ok := s.segment.(segment.PersistedSegment); ok {
			totalCount += uint64(s.FullSize())
			liveCount += uint64(s.Count())
		}
	}

	if totalCount > 0 {
		return float64(totalCount-liveCount) / float64(totalCount)
	}
	return 0
}

// subtractStrings returns set a minus elements of set b.
func subtractStrings(a, b []string) []string {
	if len(b) == 0 {
		return a
	}

	rv := make([]string, 0, len(a))
OUTER:
	for _, as := range a {
		for _, bs := range b {
			if as == bs {
				continue OUTER
			}
		}
		rv = append(rv, as)
	}
	return rv
}

func (is *IndexSnapshot) CopyTo(d index.Directory) error {
	// get the root bolt file.
	w, err := d.GetWriter(filepath.Join("store", "root.bolt"))
	if err != nil || w == nil {
		return fmt.Errorf("failed to create the root.bolt file, err: %v", err)
	}
	rootFile, ok := w.(*os.File)
	if !ok {
		return fmt.Errorf("invalid root.bolt file found")
	}

	copyBolt, err := bolt.Open(rootFile.Name(), 0600, nil)
	if err != nil {
		return err
	}
	defer func() {
		w.Close()
		if cerr := copyBolt.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// start a write transaction
	tx, err := copyBolt.Begin(true)
	if err != nil {
		return err
	}

	_, _, err = prepareBoltSnapshot(is, tx, "", is.parent.segPlugin, d)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("error backing up index snapshot: %v", err)
	}

	// commit bolt data
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error commit tx to backup root bolt: %v", err)
	}

	return copyBolt.Sync()
}

func (is *IndexSnapshot) UpdateIOStats(val uint64) {
	atomic.AddUint64(&is.parent.stats.TotBytesReadAtQueryTime, val)
}

func (is *IndexSnapshot) GetSpatialAnalyzerPlugin(typ string) (
	index.SpatialAnalyzerPlugin, error) {
	var rv index.SpatialAnalyzerPlugin
	is.m.Lock()
	rv = is.parent.spatialPlugin
	is.m.Unlock()

	if rv == nil {
		return nil, fmt.Errorf("no spatial plugin type: %s found", typ)
	}
	return rv, nil
}
