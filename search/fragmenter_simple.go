package search

import ()

const DEFAULT_FRAGMENT_SIZE = 100

type SimpleFragmenter struct {
	fragmentSize int
}

func NewSimpleFragmenter() *SimpleFragmenter {
	return &SimpleFragmenter{
		fragmentSize: DEFAULT_FRAGMENT_SIZE,
	}
}

func NewSimpleFragmenterWithSize(fragmentSize int) *SimpleFragmenter {
	return &SimpleFragmenter{
		fragmentSize: fragmentSize,
	}
}

func (s *SimpleFragmenter) Fragment(orig []byte, ot termLocations) []*Fragment {
	rv := make([]*Fragment, 0)

	maxbegin := 0
	for currTermIndex, termLocation := range ot {
		// start with with this
		// it should be the highest scoring fragment with this term first
		start := termLocation.Start
		end := start + s.fragmentSize
		if end > len(orig) {
			end = len(orig)
			// we hit end, so push back as far as we can without crossing maxbegin
			extra := s.fragmentSize - (end - start)
			if start-extra >= maxbegin {
				start -= extra
			} else {
				start = maxbegin
			}
		}
		// however, we'd rather have the tokens centered more in the frag
		// lets try to do that as best we can, without affecting the score
		// find the end of the last term in this fragment
		minend := end
		for _, innerTermLocation := range ot[currTermIndex:] {
			if innerTermLocation.End > end {
				break
			}
			minend = innerTermLocation.End
		}

		// find the smaller of the two rooms to move
		roomToMove := end - minend
		if start-maxbegin < roomToMove {
			roomToMove = start - maxbegin
		}

		offset := roomToMove / 2
		rv = append(rv, &Fragment{orig: orig, start: start - offset, end: end - offset})
		// set maxbegin to the end of the current term location
		// so that next one won't back up to include it
		maxbegin = termLocation.End

	}

	return rv
}
