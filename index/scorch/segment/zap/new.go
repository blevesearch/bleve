//  Copyright (c) 2018 Couchbase, Inc.
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

package zap

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/couchbase/vellum"
	"github.com/golang/snappy"
)

// AnalysisResultsToSegmentBase produces an in-memory zap-encoded
// SegmentBase from analysis results
func AnalysisResultsToSegmentBase(results []*index.AnalysisResult,
	chunkFactor uint32) (*SegmentBase, error) {
	var br bytes.Buffer

	s := interim{
		results:     results,
		chunkFactor: chunkFactor,
		w:           NewCountHashWriter(&br),
		FieldsMap:   map[string]uint16{},
	}

	storedIndexOffset, fieldsIndexOffset, fdvIndexOffset, dictOffsets,
		err := s.convert()
	if err != nil {
		return nil, err
	}

	sb, err := InitSegmentBase(br.Bytes(), s.w.Sum32(), chunkFactor,
		s.FieldsMap, s.FieldsInv, uint64(len(results)),
		storedIndexOffset, fieldsIndexOffset, fdvIndexOffset, dictOffsets)

	return sb, err
}

// interim holds temporary working data used while converting from
// analysis results to a zap-encoded segment
type interim struct {
	results []*index.AnalysisResult

	chunkFactor uint32

	w *CountHashWriter

	// FieldsMap adds 1 to field id to avoid zero value issues
	//  name -> field id + 1
	FieldsMap map[string]uint16

	// FieldsInv is the inverse of FieldsMap
	//  field id -> name
	FieldsInv []string

	// Term dictionaries for each field
	//  field id -> term -> postings list id + 1
	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// postings id -> bitset of docNums
	Postings []*interimDocNums

	// postings id -> bitset of docNums that have locations
	PostingsLocs []*interimDocNums

	// postings id -> freq/norm's, one for each docNum in postings
	FreqNorms [][]interimFreqNorm

	// postings id -> locs, one for each freq
	Locs [][]interimLoc

	buf0 bytes.Buffer
	tmp0 []byte
	tmp1 []byte
}

func (s *interim) grabBuf(size int) []byte {
	buf := s.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		s.tmp0 = buf
	}
	return buf[0:size]
}

type interimStoredField struct {
	vals      [][]byte
	typs      []byte
	arrayposs [][]uint64 // array positions
}

type interimFreqNorm struct {
	freq uint64
	norm float32
}

type interimLoc struct {
	fieldID   uint16
	pos       uint64
	start     uint64
	end       uint64
	arrayposs []uint64
}

// interimDocNums is a wrapper around a roaring Bitmap, and helps as a
// single roaring.AddRange(0, N) method call is faster than N
// separate, sequential Add() calls.  This helps with low cardinality
// fields (e.g., "type" field, or "gender" field), where there are
// commons runs of sequential docNum's.  The last range of docNum's is
// tracked as "plus 1" to leverage zero value convention.
type interimDocNums struct {
	bs *roaring.Bitmap // may be nil

	lastRangeBegDocNumPlus1 uint64 // inclusive
	lastRangeEndDocNumPlus1 uint64 // inclusive
}

func (d *interimDocNums) add(docNum uint64) {
	docNumPlus1 := docNum + 1

	if d.lastRangeEndDocNumPlus1 != 0 &&
		d.lastRangeEndDocNumPlus1+1 == docNumPlus1 {
		d.lastRangeEndDocNumPlus1 = docNumPlus1 // extend the last range
		return
	}

	d.incorporateLastRange()

	d.lastRangeBegDocNumPlus1 = docNumPlus1 // start a new last range
	d.lastRangeEndDocNumPlus1 = docNumPlus1
}

func (d *interimDocNums) incorporateLastRange() {
	if d.bs == nil {
		d.bs = roaring.New()
	}

	if d.lastRangeBegDocNumPlus1 == 0 {
		return // there's no last range
	}

	if d.lastRangeBegDocNumPlus1 == d.lastRangeEndDocNumPlus1 {
		d.bs.Add(uint32(d.lastRangeBegDocNumPlus1 - 1))
	} else {
		// AddRange() params represent [rangeStart, rangeEnd).
		d.bs.AddRange(d.lastRangeBegDocNumPlus1-1, d.lastRangeEndDocNumPlus1)
	}

	d.lastRangeBegDocNumPlus1 = 0
	d.lastRangeEndDocNumPlus1 = 0
}

func (s *interim) convert() (uint64, uint64, uint64, []uint64, error) {
	s.getOrDefineField("_id") // _id field is fieldID 0

	for _, result := range s.results {
		for _, field := range result.Document.CompositeFields {
			s.getOrDefineField(field.Name())
		}
		for _, field := range result.Document.Fields {
			s.getOrDefineField(field.Name())
		}
	}

	sort.Strings(s.FieldsInv[1:]) // keep _id as first field

	s.FieldsMap = make(map[string]uint16, len(s.FieldsInv))
	for fieldID, fieldName := range s.FieldsInv {
		s.FieldsMap[fieldName] = uint16(fieldID + 1)
	}

	s.IncludeDocValues = make([]bool, len(s.FieldsInv))

	s.prepareDicts()

	for _, dict := range s.DictKeys {
		sort.Strings(dict)
	}

	s.processDocuments()

	storedIndexOffset, err := s.writeStoredFields()
	if err != nil {
		return 0, 0, 0, nil, err
	}

	var fdvIndexOffset uint64
	var dictOffsets []uint64

	if len(s.results) > 0 {
		fdvIndexOffset, dictOffsets, err = s.writeDicts()
		if err != nil {
			return 0, 0, 0, nil, err
		}
	} else {
		dictOffsets = make([]uint64, len(s.FieldsInv))
	}

	fieldsIndexOffset, err := persistFields(s.FieldsInv, s.w, dictOffsets)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	return storedIndexOffset, fieldsIndexOffset, fdvIndexOffset, dictOffsets, nil
}

func (s *interim) getOrDefineField(fieldName string) int {
	fieldIDPlus1, exists := s.FieldsMap[fieldName]
	if !exists {
		fieldIDPlus1 = uint16(len(s.FieldsInv) + 1)
		s.FieldsMap[fieldName] = fieldIDPlus1
		s.FieldsInv = append(s.FieldsInv, fieldName)
		s.Dicts = append(s.Dicts, make(map[string]uint64))
		s.DictKeys = append(s.DictKeys, make([]string, 0))
	}
	return int(fieldIDPlus1 - 1)
}

// fill Dicts and DictKeys from analysis results
func (s *interim) prepareDicts() {
	var pidNext int

	numTermsPerPostingsList := make([]int, 0, 64) // key is postings list id
	numLocsPerPostingsList := make([]int, 0, 64)  // key is postings list id

	var totTFs int
	var totLocs int

	visitField := func(fieldID uint16, tfs analysis.TokenFrequencies) {
		dict := s.Dicts[fieldID]
		dictKeys := s.DictKeys[fieldID]

		for term, tf := range tfs {
			pidPlus1, exists := dict[term]
			if !exists {
				pidNext++
				pidPlus1 = uint64(pidNext)

				dict[term] = pidPlus1
				dictKeys = append(dictKeys, term)

				numTermsPerPostingsList = append(numTermsPerPostingsList, 0)
				numLocsPerPostingsList = append(numLocsPerPostingsList, 0)
			}

			pid := pidPlus1 - 1

			numTermsPerPostingsList[pid] += 1
			numLocsPerPostingsList[pid] += len(tf.Locations)

			totLocs += len(tf.Locations)
		}

		totTFs += len(tfs)

		s.DictKeys[fieldID] = dictKeys
	}

	for _, result := range s.results {
		// walk each composite field
		for _, field := range result.Document.CompositeFields {
			fieldID := uint16(s.getOrDefineField(field.Name()))
			_, tf := field.Analyze()
			visitField(fieldID, tf)
		}

		// walk each field
		for i, field := range result.Document.Fields {
			fieldID := uint16(s.getOrDefineField(field.Name()))
			tf := result.Analyzed[i]
			visitField(fieldID, tf)
		}
	}

	numPostingsLists := pidNext

	idns := make([]*interimDocNums, numPostingsLists*2)
	idnsBacking := make([]interimDocNums, numPostingsLists*2)
	for i := range idns {
		idns[i] = &idnsBacking[i]
	}

	s.Postings = idns[0:numPostingsLists]
	s.PostingsLocs = idns[numPostingsLists:]

	s.FreqNorms = make([][]interimFreqNorm, numPostingsLists)

	freqNormsBacking := make([]interimFreqNorm, totTFs)
	for pid, numTerms := range numTermsPerPostingsList {
		s.FreqNorms[pid] = freqNormsBacking[0:0]
		freqNormsBacking = freqNormsBacking[numTerms:]
	}

	s.Locs = make([][]interimLoc, numPostingsLists)

	locsBacking := make([]interimLoc, totLocs)
	for pid, numLocs := range numLocsPerPostingsList {
		s.Locs[pid] = locsBacking[0:0]
		locsBacking = locsBacking[numLocs:]
	}
}

func (s *interim) processDocuments() {
	numFields := len(s.FieldsInv)
	reuseFieldLens := make([]int, numFields)
	reuseFieldTFs := make([]analysis.TokenFrequencies, numFields)

	for docNum, result := range s.results {
		for i := 0; i < numFields; i++ { // clear these for reuse
			reuseFieldLens[i] = 0
			reuseFieldTFs[i] = nil
		}

		s.processDocument(uint64(docNum), result,
			reuseFieldLens, reuseFieldTFs)
	}
}

func (s *interim) processDocument(docNum uint64,
	result *index.AnalysisResult,
	fieldLens []int, fieldTFs []analysis.TokenFrequencies) {
	visitField := func(fieldID uint16, fieldName string,
		ln int, tf analysis.TokenFrequencies) {
		fieldLens[fieldID] += ln

		existingFreqs := fieldTFs[fieldID]
		if existingFreqs != nil {
			existingFreqs.MergeAll(fieldName, tf)
		} else {
			fieldTFs[fieldID] = tf
		}
	}

	// walk each composite field
	for _, field := range result.Document.CompositeFields {
		fieldID := uint16(s.getOrDefineField(field.Name()))
		ln, tf := field.Analyze()
		visitField(fieldID, field.Name(), ln, tf)
	}

	// walk each field
	for i, field := range result.Document.Fields {
		fieldID := uint16(s.getOrDefineField(field.Name()))
		ln := result.Length[i]
		tf := result.Analyzed[i]
		visitField(fieldID, field.Name(), ln, tf)
	}

	// now that it's been rolled up into fieldTFs, walk that
	for fieldID, tfs := range fieldTFs {
		dict := s.Dicts[fieldID]
		norm := float32(1.0 / math.Sqrt(float64(fieldLens[fieldID])))

		for term, tf := range tfs {
			pid := dict[term] - 1

			s.Postings[pid].add(docNum)

			s.FreqNorms[pid] = append(s.FreqNorms[pid],
				interimFreqNorm{
					freq: uint64(tf.Frequency()),
					norm: norm,
				})

			if len(tf.Locations) > 0 {
				s.PostingsLocs[pid].add(docNum)

				locs := s.Locs[pid]

				for _, loc := range tf.Locations {
					var locf = uint16(fieldID)
					if loc.Field != "" {
						locf = uint16(s.getOrDefineField(loc.Field))
					}
					var arrayposs []uint64
					if len(loc.ArrayPositions) > 0 {
						arrayposs = loc.ArrayPositions
					}
					locs = append(locs, interimLoc{
						fieldID:   locf,
						pos:       uint64(loc.Position),
						start:     uint64(loc.Start),
						end:       uint64(loc.End),
						arrayposs: arrayposs,
					})
				}

				s.Locs[pid] = locs
			}
		}
	}
}

func (s *interim) writeStoredFields() (
	storedIndexOffset uint64, err error) {
	metaBuf := &s.buf0
	metaEncoder := govarint.NewU64Base128Encoder(metaBuf)

	data, compressed := s.tmp0[:0], s.tmp1[:0]
	defer func() { s.tmp0, s.tmp1 = data, compressed }()

	// keyed by docNum
	docStoredOffsets := make([]uint64, len(s.results))

	// keyed by fieldID, for the current doc in the loop
	docStoredFields := map[uint16]interimStoredField{}

	for docNum, result := range s.results {
		for fieldID := range docStoredFields { // reset for next doc
			delete(docStoredFields, fieldID)
		}

		for _, field := range result.Document.Fields {
			fieldID := uint16(s.getOrDefineField(field.Name()))

			opts := field.Options()

			if opts.IsStored() {
				isf := docStoredFields[fieldID]
				isf.vals = append(isf.vals, field.Value())
				isf.typs = append(isf.typs, encodeFieldType(field))
				isf.arrayposs = append(isf.arrayposs, field.ArrayPositions())
				docStoredFields[fieldID] = isf
			}

			if opts.IncludeDocValues() {
				s.IncludeDocValues[fieldID] = true
			}
		}

		var curr int

		metaBuf.Reset()
		data = data[:0]
		compressed = compressed[:0]

		for fieldID := range s.FieldsInv {
			isf, exists := docStoredFields[uint16(fieldID)]
			if exists {
				curr, data, err = persistStoredFieldValues(
					fieldID, isf.vals, isf.typs, isf.arrayposs,
					curr, metaEncoder, data)
				if err != nil {
					return 0, err
				}
			}
		}

		metaEncoder.Close()
		metaBytes := metaBuf.Bytes()

		compressed = snappy.Encode(compressed, data)

		docStoredOffsets[docNum] = uint64(s.w.Count())

		_, err := writeUvarints(s.w,
			uint64(len(metaBytes)),
			uint64(len(compressed)))
		if err != nil {
			return 0, err
		}

		_, err = s.w.Write(metaBytes)
		if err != nil {
			return 0, err
		}

		_, err = s.w.Write(compressed)
		if err != nil {
			return 0, err
		}
	}

	storedIndexOffset = uint64(s.w.Count())

	for _, docStoredOffset := range docStoredOffsets {
		err = binary.Write(s.w, binary.BigEndian, docStoredOffset)
		if err != nil {
			return 0, err
		}
	}

	return storedIndexOffset, nil
}

func (s *interim) writeDicts() (uint64, []uint64, error) {
	dictOffsets := make([]uint64, len(s.FieldsInv))

	fdvOffsets := make([]uint64, len(s.FieldsInv))

	buf := s.grabBuf(binary.MaxVarintLen64)

	tfEncoder := newChunkedIntCoder(uint64(s.chunkFactor), uint64(len(s.results)-1))
	locEncoder := newChunkedIntCoder(uint64(s.chunkFactor), uint64(len(s.results)-1))
	fdvEncoder := newChunkedContentCoder(uint64(s.chunkFactor), uint64(len(s.results)-1))

	var docTermMap [][]byte

	s.buf0.Reset()
	builder, err := vellum.New(&s.buf0, nil)
	if err != nil {
		return 0, nil, err
	}

	for fieldID, terms := range s.DictKeys {
		if cap(docTermMap) < len(s.results) {
			docTermMap = make([][]byte, len(s.results))
		} else {
			docTermMap = docTermMap[0:len(s.results)]
			for docNum := range docTermMap { // reset the docTermMap
				docTermMap[docNum] = docTermMap[docNum][:0]
			}
		}

		dict := s.Dicts[fieldID]

		for _, term := range terms { // terms are already sorted
			pid := dict[term] - 1

			postings := s.Postings[pid]
			postings.incorporateLastRange()

			postingsLocs := s.PostingsLocs[pid]
			postingsLocs.incorporateLastRange()

			freqNorms := s.FreqNorms[pid]
			freqNormOffset := 0

			locs := s.Locs[pid]
			locOffset := 0

			postingsItr := postings.bs.Iterator()
			for postingsItr.HasNext() {
				docNum := uint64(postingsItr.Next())

				freqNorm := freqNorms[freqNormOffset]

				err = tfEncoder.Add(docNum, freqNorm.freq,
					uint64(math.Float32bits(freqNorm.norm)))
				if err != nil {
					return 0, nil, err
				}

				for i := uint64(0); i < freqNorm.freq; i++ {
					if len(locs) > 0 {
						loc := locs[locOffset]

						err = locEncoder.Add(docNum, uint64(loc.fieldID),
							loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)))
						if err != nil {
							return 0, nil, err
						}

						err = locEncoder.Add(docNum, loc.arrayposs...)
						if err != nil {
							return 0, nil, err
						}
					}

					locOffset++
				}

				freqNormOffset++

				docTermMap[docNum] = append(
					append(docTermMap[docNum], term...),
					termSeparator)
			}

			tfEncoder.Close()
			locEncoder.Close()

			postingsOffset, err := writePostings(
				postings.bs, postingsLocs.bs, tfEncoder, locEncoder,
				nil, s.w, buf)
			if err != nil {
				return 0, nil, err
			}

			if postingsOffset > uint64(0) {
				err = builder.Insert([]byte(term), postingsOffset)
				if err != nil {
					return 0, nil, err
				}
			}

			tfEncoder.Reset()
			locEncoder.Reset()
		}

		err = builder.Close()
		if err != nil {
			return 0, nil, err
		}

		// record where this dictionary starts
		dictOffsets[fieldID] = uint64(s.w.Count())

		vellumData := s.buf0.Bytes()

		// write out the length of the vellum data
		n := binary.PutUvarint(buf, uint64(len(vellumData)))
		_, err = s.w.Write(buf[:n])
		if err != nil {
			return 0, nil, err
		}

		// write this vellum to disk
		_, err = s.w.Write(vellumData)
		if err != nil {
			return 0, nil, err
		}

		// reset vellum for reuse
		s.buf0.Reset()

		err = builder.Reset(&s.buf0)
		if err != nil {
			return 0, nil, err
		}

		// write the field doc values
		if s.IncludeDocValues[fieldID] {
			for docNum, docTerms := range docTermMap {
				if len(docTerms) > 0 {
					err = fdvEncoder.Add(uint64(docNum), docTerms)
					if err != nil {
						return 0, nil, err
					}
				}
			}
			err = fdvEncoder.Close()
			if err != nil {
				return 0, nil, err
			}

			fdvOffsets[fieldID] = uint64(s.w.Count())

			_, err = fdvEncoder.Write(s.w)
			if err != nil {
				return 0, nil, err
			}

			fdvEncoder.Reset()
		} else {
			fdvOffsets[fieldID] = fieldNotUninverted
		}
	}

	fdvIndexOffset := uint64(s.w.Count())

	for _, fdvOffset := range fdvOffsets {
		n := binary.PutUvarint(buf, fdvOffset)
		_, err := s.w.Write(buf[:n])
		if err != nil {
			return 0, nil, err
		}
	}

	return fdvIndexOffset, dictOffsets, nil
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.BooleanField:
		fieldType = 'b'
	case *document.GeoPointField:
		fieldType = 'g'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}
