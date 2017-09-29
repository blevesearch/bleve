package mem

import (
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

// Dictionary is the in-memory representation of the term dictionary
type Dictionary struct {
	segment *Segment
	field   string
	fieldID uint16
}

// PostingsList returns the postings list for the specified term
func (d *Dictionary) PostingsList(term string, except *roaring.Bitmap) segment.PostingsList {
	return &PostingsList{
		dictionary: d,
		term:       term,
		postingsID: d.segment.dicts[d.fieldID][term],
		except:     except,
	}
}

// Iterator returns an iterator for this dictionary
func (d *Dictionary) Iterator() segment.DictionaryIterator {
	return &DictionaryIterator{
		d: d,
	}
}

// PrefixIterator returns an iterator which only visits terms having the
// the specified prefix
func (d *Dictionary) PrefixIterator(prefix string) segment.DictionaryIterator {
	offset := sort.SearchStrings(d.segment.dictKeys[d.fieldID], prefix)
	return &DictionaryIterator{
		d:      d,
		prefix: prefix,
		offset: offset,
	}
}

// RangeIterator returns an iterator which only visits terms between the
// start and end terms.  NOTE: bleve.index API specifies the end is inclusive.
func (d *Dictionary) RangeIterator(start, end string) segment.DictionaryIterator {
	offset := sort.SearchStrings(d.segment.dictKeys[d.fieldID], start)
	return &DictionaryIterator{
		d:      d,
		offset: offset,
		end:    end,
	}
}

// DictionaryIterator is an iterator for term dictionary
type DictionaryIterator struct {
	d      *Dictionary
	prefix string
	end    string
	offset int
}

// Next returns the next entry in the dictionary
func (d *DictionaryIterator) Next() (*index.DictEntry, error) {
	if d.offset > len(d.d.segment.dictKeys[d.d.fieldID])-1 {
		return nil, nil
	}
	next := d.d.segment.dictKeys[d.d.fieldID][d.offset]
	// check prefix
	if d.prefix != "" && !strings.HasPrefix(next, d.prefix) {
		return nil, nil
	}
	// check end (bleve.index API demands inclusive end)
	if d.end != "" && next > d.end {
		return nil, nil
	}

	d.offset++
	postingID := d.d.segment.dicts[d.d.fieldID][next]
	return &index.DictEntry{
		Term:  next,
		Count: d.d.segment.postings[postingID-1].GetCardinality(),
	}, nil
}
