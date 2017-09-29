package scorch

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index"
)

type IndexSnapshotDocIDReader struct {
	snapshot      *IndexSnapshot
	iterators     []roaring.IntIterable
	segmentOffset int
}

func (i *IndexSnapshotDocIDReader) Next() (index.IndexInternalID, error) {
	for i.segmentOffset < len(i.iterators) {
		if !i.iterators[i.segmentOffset].HasNext() {
			i.segmentOffset++
			continue
		}
		next := i.iterators[i.segmentOffset].Next()
		// make segment number into global number by adding offset
		globalOffset := i.snapshot.offsets[i.segmentOffset]
		return docNumberToBytes(uint64(next) + globalOffset), nil
	}
	return nil, nil
}

func (i *IndexSnapshotDocIDReader) Advance(ID index.IndexInternalID) (index.IndexInternalID, error) {
	// FIXME do something better
	next, err := i.Next()
	if err != nil {
		return nil, err
	}
	if next == nil {
		return nil, nil
	}
	for bytes.Compare(next, ID) < 0 {
		next, err = i.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
	}
	return next, nil
}

func (i *IndexSnapshotDocIDReader) Close() error {
	return nil
}
