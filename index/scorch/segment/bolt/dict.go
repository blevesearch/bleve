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
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/couchbaselabs/vellum"
	"github.com/couchbaselabs/vellum/regexp"
)

// Dictionary is the bolt representation of the term dictionary
type Dictionary struct {
	segment *Segment
	field   string
	fieldID uint16
	fst     *vellum.FST
}

// PostingsList returns the postings list for the specified term
func (d *Dictionary) PostingsList(term string, except *roaring.Bitmap) (segment.PostingsList, error) {
	return d.postingsList(term, except)
}

func (d *Dictionary) postingsList(term string, except *roaring.Bitmap) (*PostingsList, error) {
	rv := &PostingsList{
		dictionary: d,
		term:       term,
		except:     except,
	}

	if d.fst != nil {
		postingsID, exists, err := d.fst.Get([]byte(term))
		if err != nil {
			return nil, fmt.Errorf("vellum err: %v", err)
		}
		if exists {
			rv.postingsID = postingsID
			postingsIDKey := EncodeUvarintAscending(nil, postingsID)
			bucket := d.segment.tx.Bucket(postingsBucket)
			if bucket == nil {
				return nil, fmt.Errorf("postings bucket missing")
			}

			roaringBytes := bucket.Get(postingsIDKey)
			if roaringBytes == nil {
				return nil, fmt.Errorf("postings for postingsID %d missing", postingsID)
			}
			bitmap := roaring.NewBitmap()
			_, err = bitmap.FromBuffer(roaringBytes)
			if err != nil {
				return nil, fmt.Errorf("error loading roaring bitmap: %v", err)
			}

			rv.postings = bitmap
			rv.postingKey = postingsIDKey
		}
	}

	return rv, nil
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

	if d.fst != nil {
		r, err := regexp.New(prefix + ".*")
		if err == nil {
			itr, err := d.fst.Search(r, nil, nil)
			if err == nil {
				rv.itr = itr
			}
		}
	}

	return rv
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
		}
	}

	return rv
}

// DictionaryIterator is an iterator for term dictionary
type DictionaryIterator struct {
	d   *Dictionary
	itr vellum.Iterator
	err error
}

// Next returns the next entry in the dictionary
func (i *DictionaryIterator) Next() (*index.DictEntry, error) {
	if i.err == vellum.ErrIteratorDone {
		return nil, nil
	} else if i.err != nil {
		return nil, i.err
	}
	term, count := i.itr.Current()
	rv := &index.DictEntry{
		Term:  string(term),
		Count: count,
	}
	i.err = i.itr.Next()
	return rv, nil
}
