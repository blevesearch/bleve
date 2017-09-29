package scorch

import (
	"bytes"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type IndexSnapshotTermFieldReader struct {
	snapshot           *IndexSnapshot
	postings           []segment.PostingsList
	iterators          []segment.PostingsIterator
	segmentOffset      int
	includeFreq        bool
	includeNorm        bool
	includeTermVectors bool
}

func (i *IndexSnapshotTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	rv := preAlloced
	if rv == nil {
		rv = &index.TermFieldDoc{}
	}
	// find the next hit
	for i.segmentOffset < len(i.postings) {
		next := i.iterators[i.segmentOffset].Next()
		if next != nil {
			// make segment number into global number by adding offset
			globalOffset := i.snapshot.offsets[i.segmentOffset]
			nnum := next.Number()
			rv.ID = docNumberToBytes(nnum + globalOffset)
			if i.includeFreq {
				rv.Freq = next.Frequency()
			}
			if i.includeNorm {
				rv.Norm = next.Norm()
			}
			if i.includeTermVectors {
				locs := next.Locations()
				rv.Vectors = make([]*index.TermFieldVector, len(locs))
				for i, loc := range locs {
					rv.Vectors[i] = &index.TermFieldVector{
						Start:          loc.Start(),
						End:            loc.End(),
						Pos:            loc.Pos(),
						ArrayPositions: loc.ArrayPositions(),
						Field:          loc.Field(),
					}
				}
			}

			return rv, nil
		}
		i.segmentOffset++
	}
	return nil, nil
}

func (i *IndexSnapshotTermFieldReader) Advance(ID index.IndexInternalID, preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	// FIXME do something better
	next, err := i.Next(preAlloced)
	if err != nil {
		return nil, err
	}
	if next == nil {
		return nil, nil
	}
	for bytes.Compare(next.ID, ID) < 0 {
		next, err = i.Next(preAlloced)
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
	}
	return next, nil
}

func (i *IndexSnapshotTermFieldReader) Count() uint64 {
	var rv uint64
	for _, posting := range i.postings {
		rv += posting.Count()
	}
	return rv
}

func (i *IndexSnapshotTermFieldReader) Close() error {
	return nil
}
