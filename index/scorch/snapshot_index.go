package scorch

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type IndexSnapshot struct {
	segment  []*SegmentSnapshot
	offsets  []uint64
	internal map[string][]byte
}

func (i *IndexSnapshot) newIndexSnapshotFieldDict(field string, makeItr func(i segment.TermDictionary) segment.DictionaryIterator) (*IndexSnapshotFieldDict, error) {

	results := make(chan segment.DictionaryIterator)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			dict := segment.Dictionary(field)
			results <- makeItr(dict)
		}(index, segment)
	}

	rv := &IndexSnapshotFieldDict{
		snapshot: i,
		cursors:  make([]*segmentDictCursor, 0, len(i.segment)),
	}
	for count := 0; count < len(i.segment); count++ {
		di := <-results
		next, err := di.Next()
		if err != nil {
			return nil, err
		}
		if next != nil {
			rv.cursors = append(rv.cursors, &segmentDictCursor{
				itr:  di,
				curr: next,
			})
		}
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

func (i *IndexSnapshot) DocIDReaderAll() (index.DocIDReader, error) {
	results := make(chan *segmentDocNumsResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			results <- &segmentDocNumsResult{
				index: index,
				docs:  segment.DocNumbersLive(),
			}
		}(index, segment)
	}

	return i.newDocIDReader(results)
}

func (i *IndexSnapshot) DocIDReaderOnly(ids []string) (index.DocIDReader, error) {
	results := make(chan *segmentDocNumsResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			results <- &segmentDocNumsResult{
				index: index,
				docs:  segment.DocNumbers(ids),
			}
		}(index, segment)
	}

	return i.newDocIDReader(results)
}

type segmentDocNumsResult struct {
	index int
	docs  *roaring.Bitmap
}

func (i *IndexSnapshot) newDocIDReader(results chan *segmentDocNumsResult) (index.DocIDReader, error) {
	rv := &IndexSnapshotDocIDReader{
		snapshot:  i,
		iterators: make([]roaring.IntIterable, len(i.segment)),
	}
	for count := 0; count < len(i.segment); count++ {
		sdnr := <-results
		rv.iterators[sdnr.index] = sdnr.docs.Iterator()
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

	docNum := docInternalToNumber(next.ID)
	segmentIndex, localDocNum := i.segmentIndexAndLocalDocNumFromGlobal(docNum)

	rv = document.NewDocument(id)
	err = i.segment[segmentIndex].VisitDocument(localDocNum, func(name string, typ byte, value []byte, pos []uint64) bool {
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
	var segmentIndex uint64
	for j := 1; j < len(i.offsets); j++ {
		if docNum >= i.offsets[j] {
			segmentIndex = uint64(j)
		} else {
			break
		}
	}

	localDocNum := docNum - i.offsets[segmentIndex]
	return int(segmentIndex), localDocNum
}

func (i *IndexSnapshot) ExternalID(id index.IndexInternalID) (string, error) {
	docNum := docInternalToNumber(id)
	segmentIndex, localDocNum := i.segmentIndexAndLocalDocNumFromGlobal(docNum)

	var found bool
	var rv string
	err := i.segment[segmentIndex].VisitDocument(localDocNum, func(field string, typ byte, value []byte, pos []uint64) bool {
		if field == "_id" {
			found = true
			rv = string(value)
			return false
		}
		return true
	})
	if err != nil {
		return "", err
	}

	if found {
		return rv, nil
	}
	return "", fmt.Errorf("document number %d not found", docNum)
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
	includeNorm, includeTermVectors bool) (index.TermFieldReader, error) {

	type segmentPostingResult struct {
		index    int
		postings segment.PostingsList
	}

	results := make(chan *segmentPostingResult)
	for index, segment := range i.segment {
		go func(index int, segment *SegmentSnapshot) {
			dict := segment.Dictionary(field)
			pl := dict.PostingsList(string(term), nil)
			results <- &segmentPostingResult{
				index:    index,
				postings: pl,
			}
		}(index, segment)
	}

	rv := &IndexSnapshotTermFieldReader{
		snapshot:           i,
		postings:           make([]segment.PostingsList, len(i.segment)),
		iterators:          make([]segment.PostingsIterator, len(i.segment)),
		includeFreq:        includeFreq,
		includeNorm:        includeNorm,
		includeTermVectors: includeTermVectors,
	}
	for count := 0; count < len(i.segment); count++ {
		spr := <-results
		rv.postings[spr.index] = spr.postings
		rv.iterators[spr.index] = spr.postings.Iterator()
	}

	return rv, nil
}

func docNumberToBytes(in uint64) []byte {

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, in)
	return buf.Bytes()
}

func docInternalToNumber(in index.IndexInternalID) uint64 {
	var res uint64
	_ = binary.Read(bytes.NewReader(in), binary.BigEndian, &res)
	return res
}
