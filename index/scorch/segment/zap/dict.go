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
	"fmt"
	"regexp/syntax"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/couchbase/vellum"
	"github.com/couchbase/vellum/levenshtein"
	"github.com/couchbase/vellum/regexp"
)

// Dictionary is the zap representation of the term dictionary
type Dictionary struct {
	sb        *SegmentBase
	field     string
	fieldID   uint16
	fst       *vellum.FST
	fstReader *vellum.Reader
}

// PostingsList returns the postings list for the specified term
func (d *Dictionary) PostingsList(term []byte, except *roaring.Bitmap,
	prealloc segment.PostingsList) (segment.PostingsList, error) {
	var preallocPL *PostingsList
	pl, ok := prealloc.(*PostingsList)
	if ok && pl != nil {
		preallocPL = pl
	}
	return d.postingsList(term, except, preallocPL)
}

func (d *Dictionary) postingsList(term []byte, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	if d.fstReader == nil {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	postingsOffset, exists, err := d.fstReader.Get(term)
	if err != nil {
		return nil, fmt.Errorf("vellum err: %v", err)
	}
	if !exists {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	return d.postingsListFromOffset(postingsOffset, except, rv)
}

func (d *Dictionary) postingsListFromOffset(postingsOffset uint64, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	rv = d.postingsListInit(rv, except)

	err := rv.read(postingsOffset, d)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (d *Dictionary) postingsListInit(rv *PostingsList, except *roaring.Bitmap) *PostingsList {
	if rv == nil || rv == emptyPostingsList {
		rv = &PostingsList{}
	} else {
		postings := rv.postings
		if postings != nil {
			postings.Clear()
		}

		*rv = PostingsList{} // clear the struct

		rv.postings = postings
	}
	rv.sb = d.sb
	rv.except = except
	return rv
}

// Iterator returns an iterator for this dictionary
func (d *Dictionary) Iterator() segment.DictionaryIterator {
	rv := &DictionaryIterator{
		d: d,
	}

	if d.fst != nil {
		itr, err := d.fst.Iterator(nil, nil)
		if err == nil {
			rv.itr = itr
		} else if err != nil && err != vellum.ErrIteratorDone {
			rv.err = err
		}
	}

	return rv
}

// PrefixIterator returns an iterator which only visits terms having the
// the specified prefix
func (d *Dictionary) PrefixIterator(prefix string) segment.DictionaryIterator {
	rv := &DictionaryIterator{
		d: d,
	}

	kBeg := []byte(prefix)
	kEnd := incrementBytes(kBeg)

	if d.fst != nil {
		itr, err := d.fst.Iterator(kBeg, kEnd)
		if err == nil {
			rv.itr = itr
		} else if err != nil && err != vellum.ErrIteratorDone {
			rv.err = err
		}
	}

	return rv
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			return rv // didn't overflow, so stop
		}
	}
	return nil // overflowed
}

// RangeIterator returns an iterator which only visits terms between the
// start and end terms.  NOTE: bleve.index API specifies the end is inclusive.
func (d *Dictionary) RangeIterator(start, end string) segment.DictionaryIterator {
	rv := &DictionaryIterator{
		d: d,
	}

	// need to increment the end position to be inclusive
	endBytes := []byte(end)
	if endBytes[len(endBytes)-1] < 0xff {
		endBytes[len(endBytes)-1]++
	} else {
		endBytes = append(endBytes, 0xff)
	}

	if d.fst != nil {
		itr, err := d.fst.Iterator([]byte(start), endBytes)
		if err == nil {
			rv.itr = itr
		} else if err != nil && err != vellum.ErrIteratorDone {
			rv.err = err
		}
	}

	return rv
}

// RegexpIterator returns an iterator which only visits terms having the
// the specified regex
func (d *Dictionary) RegexpIterator(expr string) segment.DictionaryIterator {
	rv := &DictionaryIterator{
		d: d,
	}

	parsed, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		rv.err = err
		return rv
	}

	// TODO: potential optimization where syntax.Regexp supports a Simplify() API?
	// TODO: potential optimization where the literal prefix represents the,
	//       entire regexp, allowing us to use PrefixIterator(prefixTerm)?

	prefixTerm := LiteralPrefix(parsed)

	if d.fst != nil {
		r, err := regexp.NewParsedWithLimit(expr, parsed, regexp.DefaultLimit)
		if err == nil {
			var prefixBeg, prefixEnd []byte
			if prefixTerm != "" {
				prefixBeg = []byte(prefixTerm)
				prefixEnd = incrementBytes(prefixEnd)
			}

			itr, err2 := d.fst.Search(r, prefixBeg, prefixEnd)
			if err2 == nil {
				rv.itr = itr
			} else if err2 != nil && err2 != vellum.ErrIteratorDone {
				rv.err = err2
			}
		} else {
			rv.err = err
		}
	}

	return rv
}

// FuzzyIterator returns an iterator which only visits terms having the
// the specified edit/levenshtein distance
func (d *Dictionary) FuzzyIterator(term string,
	fuzziness int) segment.DictionaryIterator {
	rv := &DictionaryIterator{
		d: d,
	}

	if d.fst != nil {
		la, err := levenshtein.New(term, fuzziness)
		if err == nil {
			itr, err2 := d.fst.Search(la, nil, nil)
			if err2 == nil {
				rv.itr = itr
			} else if err2 != nil && err2 != vellum.ErrIteratorDone {
				rv.err = err2
			}
		} else {
			rv.err = err
		}
	}

	return rv
}

func (d *Dictionary) OnlyIterator(onlyTerms [][]byte,
	includeCount bool) segment.DictionaryIterator {

	rv := &DictionaryIterator{
		d:         d,
		omitCount: !includeCount,
	}

	var buf bytes.Buffer
	builder, err := vellum.New(&buf, nil)
	if err != nil {
		rv.err = err
		return rv
	}
	for _, term := range onlyTerms {
		err = builder.Insert(term, 0)
		if err != nil {
			rv.err = err
			return rv
		}
	}
	err = builder.Close()
	if err != nil {
		rv.err = err
		return rv
	}

	onlyFST, err := vellum.Load(buf.Bytes())
	if err != nil {
		rv.err = err
		return rv
	}

	itr, err := d.fst.Search(onlyFST, nil, nil)
	if err == nil {
		rv.itr = itr
	} else if err != nil && err != vellum.ErrIteratorDone {
		rv.err = err
	}

	return rv
}

// DictionaryIterator is an iterator for term dictionary
type DictionaryIterator struct {
	d         *Dictionary
	itr       vellum.Iterator
	err       error
	tmp       PostingsList
	entry     index.DictEntry
	omitCount bool
}

// Next returns the next entry in the dictionary
func (i *DictionaryIterator) Next() (*index.DictEntry, error) {
	if i.err != nil && i.err != vellum.ErrIteratorDone {
		return nil, i.err
	} else if i.itr == nil || i.err == vellum.ErrIteratorDone {
		return nil, nil
	}
	term, postingsOffset := i.itr.Current()
	i.entry.Term = string(term)
	if !i.omitCount {
		i.err = i.tmp.read(postingsOffset, i.d)
		if i.err != nil {
			return nil, i.err
		}
		i.entry.Count = i.tmp.Count()
	}
	i.err = i.itr.Next()
	return &i.entry, nil
}
