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

package mem

import (
	"reflect"

	"github.com/RoaringBitmap/roaring"
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

// PostingsList is an in-memory represenation of a postings list
type PostingsList struct {
	dictionary *Dictionary
	term       string
	postingsID uint64
	except     *roaring.Bitmap
}

func (p *PostingsList) Size() int {
	sizeInBytes := reflectStaticSizePostingsList + size.SizeOfPtr

	if p.dictionary != nil {
		sizeInBytes += p.dictionary.Size()
	}

	if p.except != nil {
		sizeInBytes += int(p.except.GetSizeInBytes())
	}

	return sizeInBytes
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	var rv uint64
	if p.postingsID > 0 {
		rv = p.dictionary.segment.Postings[p.postingsID-1].GetCardinality()
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

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator() segment.PostingsIterator {
	return p.InitIterator(nil)
}
func (p *PostingsList) InitIterator(prealloc *PostingsIterator) *PostingsIterator {
	rv := prealloc
	if rv == nil {
		rv = &PostingsIterator{postings: p}
	} else {
		*rv = PostingsIterator{postings: p}
	}

	if p.postingsID > 0 {
		allbits := p.dictionary.segment.Postings[p.postingsID-1]
		rv.locations = p.dictionary.segment.PostingsLocs[p.postingsID-1]
		rv.all = allbits.Iterator()
		if p.except != nil {
			allExcept := allbits.Clone()
			allExcept.AndNot(p.except)
			rv.actual = allExcept.Iterator()
		} else {
			rv.actual = allbits.Iterator()
		}
	}

	return rv
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings  *PostingsList
	all       roaring.IntIterable
	locations *roaring.Bitmap
	offset    int
	locoffset int
	actual    roaring.IntIterable
	reuse     Posting
}

func (i *PostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + size.SizeOfPtr

	if i.locations != nil {
		sizeInBytes += int(i.locations.GetSizeInBytes())
	}

	return sizeInBytes
}

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) Next() (segment.Posting, error) {
	if i.actual == nil || !i.actual.HasNext() {
		return nil, nil
	}
	n := i.actual.Next()
	allN := i.all.Next()

	// n is the next actual hit (excluding some postings)
	// allN is the next hit in the full postings
	// if they don't match, adjust offsets to factor in item we're skipping over
	// incr the all iterator, and check again
	for allN != n {
		i.locoffset += int(i.postings.dictionary.segment.Freqs[i.postings.postingsID-1][i.offset])
		i.offset++
		allN = i.all.Next()
	}
	i.reuse = Posting{
		iterator:  i,
		docNum:    uint64(n),
		offset:    i.offset,
		locoffset: i.locoffset,
		hasLoc:    i.locations.Contains(n),
	}
	i.locoffset += int(i.postings.dictionary.segment.Freqs[i.postings.postingsID-1][i.offset])
	i.offset++
	return &i.reuse, nil
}

// Posting is a single entry in a postings list
type Posting struct {
	iterator  *PostingsIterator
	docNum    uint64
	offset    int
	locoffset int
	hasLoc    bool
}

func (p *Posting) Size() int {
	sizeInBytes := reflectStaticSizePosting + size.SizeOfPtr

	if p.iterator != nil {
		sizeInBytes += p.iterator.Size()
	}

	return sizeInBytes
}

// Number returns the document number of this posting in this segment
func (p *Posting) Number() uint64 {
	return p.docNum
}

// Frequency returns the frequence of occurance of this term in this doc/field
func (p *Posting) Frequency() uint64 {
	return p.iterator.postings.dictionary.segment.Freqs[p.iterator.postings.postingsID-1][p.offset]
}

// Norm returns the normalization factor for this posting
func (p *Posting) Norm() float64 {
	return float64(p.iterator.postings.dictionary.segment.Norms[p.iterator.postings.postingsID-1][p.offset])
}

// Locations returns the location information for each occurance
func (p *Posting) Locations() []segment.Location {
	if !p.hasLoc {
		return nil
	}
	freq := int(p.Frequency())
	rv := make([]segment.Location, freq)
	for i := 0; i < freq; i++ {
		rv[i] = &Location{
			p:      p,
			offset: p.locoffset + i,
		}
	}
	return rv
}

// Location represents the location of a single occurance
type Location struct {
	p      *Posting
	offset int
}

func (l *Location) Size() int {
	sizeInBytes := reflectStaticSizeLocation
	if l.p != nil {
		sizeInBytes += l.p.Size()
	}

	return sizeInBytes
}

// Field returns the name of the field (useful in composite fields to know
// which original field the value came from)
func (l *Location) Field() string {
	return l.p.iterator.postings.dictionary.segment.FieldsInv[l.p.iterator.postings.dictionary.segment.Locfields[l.p.iterator.postings.postingsID-1][l.offset]]
}

// Start returns the start byte offset of this occurance
func (l *Location) Start() uint64 {
	return l.p.iterator.postings.dictionary.segment.Locstarts[l.p.iterator.postings.postingsID-1][l.offset]
}

// End returns the end byte offset of this occurance
func (l *Location) End() uint64 {
	return l.p.iterator.postings.dictionary.segment.Locends[l.p.iterator.postings.postingsID-1][l.offset]
}

// Pos returns the 1-based phrase position of this occurance
func (l *Location) Pos() uint64 {
	return l.p.iterator.postings.dictionary.segment.Locpos[l.p.iterator.postings.postingsID-1][l.offset]
}

// ArrayPositions returns the array position vector associated with this occurance
func (l *Location) ArrayPositions() []uint64 {
	return l.p.iterator.postings.dictionary.segment.Locarraypos[l.p.iterator.postings.postingsID-1][l.offset]
}
