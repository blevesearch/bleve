//go:build vectors
// +build vectors

package scorch

import segment "github.com/blevesearch/scorch_segment_api/v2"

type emptyVecPostingsIterator struct{}

func (e *emptyVecPostingsIterator) Next() (segment.VecPosting, error) {
	return nil, nil
}

func (e *emptyVecPostingsIterator) Advance(uint64) (segment.VecPosting, error) {
	return nil, nil
}

func (e *emptyVecPostingsIterator) Size() int {
	return 0
}

func (e *emptyVecPostingsIterator) BytesRead() uint64 {
	return 0
}

func (e *emptyVecPostingsIterator) ResetBytesRead(uint64) {}

func (e *emptyVecPostingsIterator) BytesWritten() uint64 { return 0 }

var anemptyVecPostingsIterator = &emptyVecPostingsIterator{}
