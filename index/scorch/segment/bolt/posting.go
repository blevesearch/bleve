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

package bolt

import (
	"bytes"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/boltdb/bolt"
)

// PostingsList is an in-memory represenation of a postings list
type PostingsList struct {
	dictionary *Dictionary
	term       string
	postingsID uint64
	postings   *roaring.Bitmap
	except     *roaring.Bitmap
	postingKey []byte
}

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator() segment.PostingsIterator {
	rv := &PostingsIterator{
		postings: p,
	}
	if p.postings != nil {
		detailsBucket := p.dictionary.segment.tx.Bucket(postingDetailsBucket)
		rv.detailBucket = detailsBucket.Bucket(p.postingKey)
		rv.all = p.postings.Iterator()
		if p.except != nil {
			allExcept := p.postings.Clone()
			allExcept.AndNot(p.except)
			rv.actual = allExcept.Iterator()
		} else {
			rv.actual = p.postings.Iterator()
		}
	}

	return rv
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	var rv uint64
	if p.postings != nil {
		rv = p.postings.GetCardinality()
		if p.except != nil {
			except := p.except.GetCardinality()
			if except > rv {
				// avoid underflow
				except = rv
			}
			rv -= except
		}
	}
	return rv
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings     *PostingsList
	all          roaring.IntIterable
	offset       int
	locoffset    int
	actual       roaring.IntIterable
	detailBucket *bolt.Bucket

	currChunk         uint32
	currChunkFreqNorm []byte
	currChunkLoc      []byte
	freqNormDecoder   *govarint.Base128Decoder
	locDecoder        *govarint.Base128Decoder
}

func (i *PostingsIterator) loadChunk(chunk int) error {
	// load correct chunk bytes
	chunkID := segment.EncodeUvarintAscending(nil, uint64(chunk))
	chunkBucket := i.detailBucket.Bucket(chunkID)
	if chunkBucket == nil {
		return fmt.Errorf("chunk %d missing", chunkID)
	}
	i.currChunkFreqNorm = chunkBucket.Get(freqNormKey)
	i.freqNormDecoder = govarint.NewU64Base128Decoder(bytes.NewReader(i.currChunkFreqNorm))
	i.currChunkLoc = chunkBucket.Get(locKey)
	i.locDecoder = govarint.NewU64Base128Decoder(bytes.NewReader(i.currChunkLoc))
	i.currChunk = uint32(chunk)
	return nil
}

func (i *PostingsIterator) readFreqNorm() (uint64, uint64, error) {
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
		l.field = i.postings.dictionary.segment.fieldsInv[fieldID]
		l.pos = pos
		l.start = start
		l.end = end
		if numArrayPos > 0 {
			l.ap = make([]uint64, int(numArrayPos))
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
	if i.actual == nil || !i.actual.HasNext() {
		return nil, nil
	}
	n := i.actual.Next()
	nChunk := n / i.postings.dictionary.segment.chunkFactor
	allN := i.all.Next()
	allNChunk := allN / i.postings.dictionary.segment.chunkFactor

	// n is the next actual hit (excluding some postings)
	// allN is the next hit in the full postings
	// if they don't match, adjust offsets to factor in item we're skipping over
	// incr the all iterator, and check again
	for allN != n {

		// in different chunks, reset offsets
		if allNChunk != nChunk {
			i.locoffset = 0
			i.offset = 0
		} else {

			if i.currChunk != nChunk || i.currChunkFreqNorm == nil {
				err := i.loadChunk(int(nChunk))
				if err != nil {
					return nil, fmt.Errorf("error loading chunk: %v", err)
				}
			}

			// read off freq/offsets even though we don't care about them
			freq, _, err := i.readFreqNorm()
			if err != nil {
				return nil, err
			}
			if i.postings.dictionary.segment.fieldsLoc[i.postings.dictionary.fieldID] {
				for j := 0; j < int(freq); j++ {
					err := i.readLocation(nil)
					if err != nil {
						return nil, err
					}
				}
			}

			// in same chunk, need to account for offsets
			i.offset++
		}

		allN = i.all.Next()
	}

	if i.currChunk != nChunk || i.currChunkFreqNorm == nil {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return nil, fmt.Errorf("error loading chunk: %v", err)
		}
	}

	rv := &Posting{
		iterator: i,
		docNum:   uint64(n),
	}

	var err error
	var normBits uint64
	rv.freq, normBits, err = i.readFreqNorm()
	if err != nil {
		return nil, err
	}
	rv.norm = math.Float32frombits(uint32(normBits))
	if i.postings.dictionary.segment.fieldsLoc[i.postings.dictionary.fieldID] {
		// read off 'freq' locations
		rv.locs = make([]segment.Location, rv.freq)
		locs := make([]Location, rv.freq)
		for j := 0; j < int(rv.freq); j++ {
			err := i.readLocation(&locs[j])
			if err != nil {
				return nil, err
			}
			rv.locs[j] = &locs[j]
		}
	}

	return rv, nil
}

// Posting is a single entry in a postings list
type Posting struct {
	iterator *PostingsIterator
	docNum   uint64

	freq uint64
	norm float32
	locs []segment.Location
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
