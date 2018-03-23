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

package zap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/blevesearch/bleve/size"
)

var reflectStaticSizePostingsList int
var reflectStaticSizePostingsIterator int
var reflectStaticSizePosting int
var reflectStaticSizeLocation int

func init() {
	var pl PostingsList
	reflectStaticSizePostingsList = int(reflect.TypeOf(pl).Size())
	var pi PostingsIterator
	reflectStaticSizePostingsIterator = int(reflect.TypeOf(pi).Size())
	var p Posting
	reflectStaticSizePosting = int(reflect.TypeOf(p).Size())
	var l Location
	reflectStaticSizeLocation = int(reflect.TypeOf(l).Size())
}

// FST or vellum value (uint64) encoding is determined by the top two
// highest-order or most significant bits...
//
//  encoding  : MSB
//  name      : 63  62  61...to...bit #0 (LSB)
//  ----------+---+---+---------------------------------------------------
//   general  : 0 | 0 | 62-bits of postingsOffset.
//   ~        : 0 | 1 | reserved for future.
//   1-hit    : 1 | 0 | 31-bits of positive float31 norm | 31-bits docNum.
//   ~        : 1 | 1 | reserved for future.
//
// Encoding "general" is able to handle all cases, where the
// postingsOffset points to more information about the postings for
// the term.
//
// Encoding "1-hit" is used to optimize a commonly seen case when a
// term has only a single hit.  For example, a term in the _id field
// will have only 1 hit.  The "1-hit" encoding is used for a term
// in a field when...
//
// - term vector info is disabled for that field;
// - and, the term appears in only a single doc for that field;
// - and, the term's freq is exactly 1 in that single doc for that field;
// - and, the docNum must fit into 31-bits;
//
// Otherwise, the "general" encoding is used instead.
//
// In the "1-hit" encoding, the field in that single doc may have
// other terms, which is supported in the "1-hit" encoding by the
// positive float31 norm.

const FSTValEncodingMask = uint64(0xc000000000000000)
const FSTValEncodingGeneral = uint64(0x0000000000000000)
const FSTValEncoding1Hit = uint64(0x8000000000000000)

func FSTValEncode1Hit(docNum uint64, normBits uint64) uint64 {
	return FSTValEncoding1Hit | ((mask31Bits & normBits) << 31) | (mask31Bits & docNum)
}

func FSTValDecode1Hit(v uint64) (docNum uint64, normBits uint64) {
	return (mask31Bits & v), (mask31Bits & (v >> 31))
}

const mask31Bits = uint64(0x000000007fffffff)

func under32Bits(x uint64) bool {
	return x <= mask31Bits
}

const docNum1HitFinished = math.MaxUint64

// PostingsList is an in-memory represenation of a postings list
type PostingsList struct {
	sb             *SegmentBase
	postingsOffset uint64
	freqOffset     uint64
	locOffset      uint64
	locBitmap      *roaring.Bitmap
	postings       *roaring.Bitmap
	except         *roaring.Bitmap

	// when normBits1Hit != 0, then this postings list came from a
	// 1-hit encoding, and only the docNum1Hit & normBits1Hit apply
	docNum1Hit   uint64
	normBits1Hit uint64
}

func (p *PostingsList) Size() int {
	sizeInBytes := reflectStaticSizePostingsList + size.SizeOfPtr

	if p.except != nil {
		sizeInBytes += int(p.except.GetSizeInBytes())
	}

	return sizeInBytes
}

func (p *PostingsList) OrInto(receiver *roaring.Bitmap) {
	if p.normBits1Hit != 0 {
		receiver.Add(uint32(p.docNum1Hit))
		return
	}

	if p.postings != nil {
		receiver.Or(p.postings)
	}
}

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator() segment.PostingsIterator {
	return p.iterator(nil)
}

func (p *PostingsList) iterator(rv *PostingsIterator) *PostingsIterator {
	if rv == nil {
		rv = &PostingsIterator{}
	} else {
		freqNormReader := rv.freqNormReader
		if freqNormReader != nil {
			freqNormReader.Reset([]byte(nil))
		}
		freqNormDecoder := rv.freqNormDecoder

		locReader := rv.locReader
		if locReader != nil {
			locReader.Reset([]byte(nil))
		}
		locDecoder := rv.locDecoder

		freqChunkOffsets := rv.freqChunkOffsets[:0]
		locChunkOffsets := rv.locChunkOffsets[:0]

		buf := rv.buf

		*rv = PostingsIterator{} // clear the struct

		rv.freqNormReader = freqNormReader
		rv.freqNormDecoder = freqNormDecoder

		rv.locReader = locReader
		rv.locDecoder = locDecoder

		rv.freqChunkOffsets = freqChunkOffsets
		rv.locChunkOffsets = locChunkOffsets

		rv.buf = buf
	}
	rv.postings = p

	if p.normBits1Hit != 0 {
		// "1-hit" encoding
		rv.docNum1Hit = p.docNum1Hit
		rv.normBits1Hit = p.normBits1Hit

		if p.except != nil && p.except.Contains(uint32(rv.docNum1Hit)) {
			rv.docNum1Hit = docNum1HitFinished
		}

		return rv
	}

	// "general" encoding, check if empty
	if p.postings == nil {
		return rv
	}

	// prepare the freq chunk details
	var n uint64
	var read int
	var numFreqChunks uint64
	numFreqChunks, read = binary.Uvarint(p.sb.mem[p.freqOffset+n : p.freqOffset+n+binary.MaxVarintLen64])
	n += uint64(read)
	if cap(rv.freqChunkOffsets) >= int(numFreqChunks) {
		rv.freqChunkOffsets = rv.freqChunkOffsets[:int(numFreqChunks)]
	} else {
		rv.freqChunkOffsets = make([]uint64, int(numFreqChunks))
	}
	for i := 0; i < int(numFreqChunks); i++ {
		rv.freqChunkOffsets[i], read = binary.Uvarint(p.sb.mem[p.freqOffset+n : p.freqOffset+n+binary.MaxVarintLen64])
		n += uint64(read)
	}
	rv.freqChunkStart = p.freqOffset + n

	// prepare the loc chunk details
	n = 0
	var numLocChunks uint64
	numLocChunks, read = binary.Uvarint(p.sb.mem[p.locOffset+n : p.locOffset+n+binary.MaxVarintLen64])
	n += uint64(read)
	if cap(rv.locChunkOffsets) >= int(numLocChunks) {
		rv.locChunkOffsets = rv.locChunkOffsets[:int(numLocChunks)]
	} else {
		rv.locChunkOffsets = make([]uint64, int(numLocChunks))
	}
	for i := 0; i < int(numLocChunks); i++ {
		rv.locChunkOffsets[i], read = binary.Uvarint(p.sb.mem[p.locOffset+n : p.locOffset+n+binary.MaxVarintLen64])
		n += uint64(read)
	}
	rv.locChunkStart = p.locOffset + n

	rv.locBitmap = p.locBitmap

	rv.all = p.postings.Iterator()
	if p.except != nil {
		allExcept := roaring.AndNot(p.postings, p.except)
		rv.actual = allExcept.Iterator()
	} else {
		rv.actual = p.postings.Iterator()
	}

	return rv
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	var n uint64
	if p.normBits1Hit != 0 {
		n = 1
	} else if p.postings != nil {
		n = p.postings.GetCardinality()
	}
	var e uint64
	if p.except != nil {
		e = p.except.GetCardinality()
	}
	if n <= e {
		return 0
	}
	return n - e
}

func (rv *PostingsList) read(postingsOffset uint64, d *Dictionary) error {
	rv.postingsOffset = postingsOffset

	// handle "1-hit" encoding special case
	if rv.postingsOffset&FSTValEncodingMask == FSTValEncoding1Hit {
		return rv.init1Hit(postingsOffset)
	}

	// read the location of the freq/norm details
	var n uint64
	var read int

	rv.freqOffset, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+binary.MaxVarintLen64])
	n += uint64(read)

	rv.locOffset, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	var locBitmapOffset uint64
	locBitmapOffset, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	var locBitmapLen uint64
	locBitmapLen, read = binary.Uvarint(d.sb.mem[locBitmapOffset : locBitmapOffset+binary.MaxVarintLen64])

	locRoaringBytes := d.sb.mem[locBitmapOffset+uint64(read) : locBitmapOffset+uint64(read)+locBitmapLen]

	if rv.locBitmap == nil {
		rv.locBitmap = roaring.NewBitmap()
	}
	_, err := rv.locBitmap.FromBuffer(locRoaringBytes)
	if err != nil {
		return fmt.Errorf("error loading roaring bitmap of locations with hits: %v", err)
	}

	var postingsLen uint64
	postingsLen, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	roaringBytes := d.sb.mem[postingsOffset+n : postingsOffset+n+postingsLen]

	if rv.postings == nil {
		rv.postings = roaring.NewBitmap()
	}
	_, err = rv.postings.FromBuffer(roaringBytes)
	if err != nil {
		return fmt.Errorf("error loading roaring bitmap: %v", err)
	}

	return nil
}

func (rv *PostingsList) init1Hit(fstVal uint64) error {
	docNum, normBits := FSTValDecode1Hit(fstVal)

	rv.docNum1Hit = docNum
	rv.normBits1Hit = normBits

	return nil
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings *PostingsList
	all      roaring.IntIterable
	actual   roaring.IntIterable

	currChunk         uint32
	currChunkFreqNorm []byte
	currChunkLoc      []byte
	freqNormDecoder   *govarint.Base128Decoder
	freqNormReader    *bytes.Reader
	locDecoder        *govarint.Base128Decoder
	locReader         *bytes.Reader

	freqChunkOffsets []uint64
	freqChunkStart   uint64

	locChunkOffsets []uint64
	locChunkStart   uint64

	locBitmap *roaring.Bitmap

	next     Posting    // reused across Next() calls
	nextLocs []Location // reused across Next() calls

	docNum1Hit   uint64
	normBits1Hit uint64

	buf []byte
}

func (i *PostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + size.SizeOfPtr +
		len(i.currChunkFreqNorm) +
		len(i.currChunkLoc) +
		len(i.freqChunkOffsets)*size.SizeOfUint64 +
		len(i.locChunkOffsets)*size.SizeOfUint64 +
		i.next.Size()

	if i.locBitmap != nil {
		sizeInBytes += int(i.locBitmap.GetSizeInBytes())
	}

	for _, entry := range i.nextLocs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

func (i *PostingsIterator) loadChunk(chunk int) error {
	if chunk >= len(i.freqChunkOffsets) || chunk >= len(i.locChunkOffsets) {
		return fmt.Errorf("tried to load chunk that doesn't exist %d/(%d %d)", chunk, len(i.freqChunkOffsets), len(i.locChunkOffsets))
	}

	end, start := i.freqChunkStart, i.freqChunkStart
	s, e := readChunkBoundary(chunk, i.freqChunkOffsets)
	start += s
	end += e
	i.currChunkFreqNorm = i.postings.sb.mem[start:end]
	if i.freqNormReader == nil {
		i.freqNormReader = bytes.NewReader(i.currChunkFreqNorm)
		i.freqNormDecoder = govarint.NewU64Base128Decoder(i.freqNormReader)
	} else {
		i.freqNormReader.Reset(i.currChunkFreqNorm)
	}

	end, start = i.locChunkStart, i.locChunkStart
	s, e = readChunkBoundary(chunk, i.locChunkOffsets)
	start += s
	end += e
	i.currChunkLoc = i.postings.sb.mem[start:end]
	if i.locReader == nil {
		i.locReader = bytes.NewReader(i.currChunkLoc)
		i.locDecoder = govarint.NewU64Base128Decoder(i.locReader)
	} else {
		i.locReader.Reset(i.currChunkLoc)
	}

	i.currChunk = uint32(chunk)
	return nil
}

func (i *PostingsIterator) readFreqNorm() (uint64, uint64, error) {
	if i.normBits1Hit != 0 {
		return 1, i.normBits1Hit, nil
	}

	freq, err := i.freqNormDecoder.GetU64()
	if err != nil {
		return 0, 0, fmt.Errorf("error reading frequency: %v", err)
	}
	normBits, err := i.freqNormDecoder.GetU64()
	if err != nil {
		return 0, 0, fmt.Errorf("error reading norm: %v", err)
	}
	return freq, normBits, err
}

// readLocation processes all the integers on the stream representing a single
// location.  if you care about it, pass in a non-nil location struct, and we
// will fill it.  if you don't care about it, pass in nil and we safely consume
// the contents.
func (i *PostingsIterator) readLocation(l *Location) error {
	// read off field
	fieldID, err := i.locDecoder.GetU64()
	if err != nil {
		return fmt.Errorf("error reading location field: %v", err)
	}
	// read off pos
	pos, err := i.locDecoder.GetU64()
	if err != nil {
		return fmt.Errorf("error reading location pos: %v", err)
	}
	// read off start
	start, err := i.locDecoder.GetU64()
	if err != nil {
		return fmt.Errorf("error reading location start: %v", err)
	}
	// read off end
	end, err := i.locDecoder.GetU64()
	if err != nil {
		return fmt.Errorf("error reading location end: %v", err)
	}
	// read off num array pos
	numArrayPos, err := i.locDecoder.GetU64()
	if err != nil {
		return fmt.Errorf("error reading location num array pos: %v", err)
	}

	// group these together for less branching
	if l != nil {
		l.field = i.postings.sb.fieldsInv[fieldID]
		l.pos = pos
		l.start = start
		l.end = end
		if numArrayPos > 0 {
			l.ap = make([]uint64, int(numArrayPos))
		} else {
			l.ap = l.ap[:0]
		}
	}

	// read off array positions
	for k := 0; k < int(numArrayPos); k++ {
		ap, err := i.locDecoder.GetU64()
		if err != nil {
			return fmt.Errorf("error reading array position: %v", err)
		}
		if l != nil {
			l.ap[k] = ap
		}
	}

	return nil
}

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) Next() (segment.Posting, error) {
	docNum, exists, err := i.nextDocNum()
	if err != nil || !exists {
		return nil, err
	}

	reuseLocs := i.next.locs // hold for reuse before struct clearing
	i.next = Posting{}       // clear the struct
	rv := &i.next
	rv.docNum = docNum

	var normBits uint64
	rv.freq, normBits, err = i.readFreqNorm()
	if err != nil {
		return nil, err
	}
	rv.norm = math.Float32frombits(uint32(normBits))

	if i.locBitmap != nil && i.locBitmap.Contains(uint32(docNum)) {
		// read off 'freq' locations, into reused slices
		if cap(i.nextLocs) >= int(rv.freq) {
			i.nextLocs = i.nextLocs[0:rv.freq]
		} else {
			i.nextLocs = make([]Location, rv.freq)
		}
		if cap(reuseLocs) >= int(rv.freq) {
			rv.locs = reuseLocs[0:rv.freq]
		} else {
			rv.locs = make([]segment.Location, rv.freq)
		}
		for j := 0; j < int(rv.freq); j++ {
			err := i.readLocation(&i.nextLocs[j])
			if err != nil {
				return nil, err
			}
			rv.locs[j] = &i.nextLocs[j]
		}
	}

	return rv, nil
}

// nextBytes returns the docNum and the encoded freq & loc bytes for
// the next posting
func (i *PostingsIterator) nextBytes() (
	docNumOut uint64, freq uint64, normBits uint64,
	bytesFreqNorm []byte, bytesLoc []byte, err error) {
	docNum, exists, err := i.nextDocNum()
	if err != nil || !exists {
		return 0, 0, 0, nil, nil, err
	}

	if i.normBits1Hit != 0 {
		if i.buf == nil {
			i.buf = make([]byte, binary.MaxVarintLen64*2)
		}
		n := binary.PutUvarint(i.buf, uint64(1))
		n += binary.PutUvarint(i.buf, i.normBits1Hit)
		return docNum, uint64(1), i.normBits1Hit, i.buf[:n], nil, nil
	}

	startFreqNorm := len(i.currChunkFreqNorm) - i.freqNormReader.Len()

	freq, normBits, err = i.readFreqNorm()
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}

	endFreqNorm := len(i.currChunkFreqNorm) - i.freqNormReader.Len()
	bytesFreqNorm = i.currChunkFreqNorm[startFreqNorm:endFreqNorm]

	if i.locBitmap != nil && i.locBitmap.Contains(uint32(docNum)) {
		startLoc := len(i.currChunkLoc) - i.locReader.Len()

		for j := uint64(0); j < freq; j++ {
			err := i.readLocation(nil)
			if err != nil {
				return 0, 0, 0, nil, nil, err
			}
		}

		endLoc := len(i.currChunkLoc) - i.locReader.Len()
		bytesLoc = i.currChunkLoc[startLoc:endLoc]
	}

	return docNum, freq, normBits, bytesFreqNorm, bytesLoc, nil
}

// nextDocNum returns the next docNum on the postings list, and also
// sets up the currChunk / loc related fields of the iterator.
func (i *PostingsIterator) nextDocNum() (uint64, bool, error) {
	if i.normBits1Hit != 0 {
		if i.docNum1Hit == docNum1HitFinished {
			return 0, false, nil
		}
		docNum := i.docNum1Hit
		i.docNum1Hit = docNum1HitFinished // consume our 1-hit docNum
		return docNum, true, nil
	}

	if i.actual == nil || !i.actual.HasNext() {
		return 0, false, nil
	}

	n := i.actual.Next()
	allN := i.all.Next()

	nChunk := n / i.postings.sb.chunkFactor
	allNChunk := allN / i.postings.sb.chunkFactor

	// n is the next actual hit (excluding some postings), and
	// allN is the next hit in the full postings, and
	// if they don't match, move 'all' forwards until they do
	for allN != n {
		// in the same chunk, so move the freq/norm/loc decoders forward
		if allNChunk == nChunk {
			if i.currChunk != nChunk || i.currChunkFreqNorm == nil {
				err := i.loadChunk(int(nChunk))
				if err != nil {
					return 0, false, fmt.Errorf("error loading chunk: %v", err)
				}
			}

			// read off freq/offsets even though we don't care about them
			freq, _, err := i.readFreqNorm()
			if err != nil {
				return 0, false, err
			}
			if i.locBitmap.Contains(allN) {
				for j := 0; j < int(freq); j++ {
					err := i.readLocation(nil)
					if err != nil {
						return 0, false, err
					}
				}
			}
		}

		allN = i.all.Next()
		allNChunk = allN / i.postings.sb.chunkFactor
	}

	if i.currChunk != nChunk || i.currChunkFreqNorm == nil {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return 0, false, fmt.Errorf("error loading chunk: %v", err)
		}
	}

	return uint64(n), true, nil
}

// Posting is a single entry in a postings list
type Posting struct {
	docNum uint64
	freq   uint64
	norm   float32
	locs   []segment.Location
}

func (p *Posting) Size() int {
	sizeInBytes := reflectStaticSizePosting

	for _, entry := range p.locs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// Number returns the document number of this posting in this segment
func (p *Posting) Number() uint64 {
	return p.docNum
}

// Frequency returns the frequence of occurance of this term in this doc/field
func (p *Posting) Frequency() uint64 {
	return p.freq
}

// Norm returns the normalization factor for this posting
func (p *Posting) Norm() float64 {
	return float64(p.norm)
}

// Locations returns the location information for each occurance
func (p *Posting) Locations() []segment.Location {
	return p.locs
}

// Location represents the location of a single occurance
type Location struct {
	field string
	pos   uint64
	start uint64
	end   uint64
	ap    []uint64
}

func (l *Location) Size() int {
	return reflectStaticSizeLocation +
		len(l.field) +
		len(l.ap)*size.SizeOfUint64
}

// Field returns the name of the field (useful in composite fields to know
// which original field the value came from)
func (l *Location) Field() string {
	return l.field
}

// Start returns the start byte offset of this occurance
func (l *Location) Start() uint64 {
	return l.start
}

// End returns the end byte offset of this occurance
func (l *Location) End() uint64 {
	return l.end
}

// Pos returns the 1-based phrase position of this occurance
func (l *Location) Pos() uint64 {
	return l.pos
}

// ArrayPositions returns the array position vector associated with this occurance
func (l *Location) ArrayPositions() []uint64 {
	return l.ap
}
