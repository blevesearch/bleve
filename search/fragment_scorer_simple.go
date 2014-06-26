package search

import ()

// SimpleFragmentScorer will score fragments by how many
// unique terms occur in the fragment with no regard for
// any boost values used in the original query
type SimpleFragmentScorer struct {
	tlm TermLocationMap
}

func NewSimpleFragmentScorer(tlm TermLocationMap) *SimpleFragmentScorer {
	return &SimpleFragmentScorer{
		tlm: tlm,
	}
}

func (s *SimpleFragmentScorer) Score(f *Fragment) {
	score := 0.0
OUTER:
	for _, locations := range s.tlm {
		for _, location := range locations {
			if int(location.Start) >= f.start && int(location.End) <= f.end {
				score += 1.0
				// once we find a term in the fragment
				// don't care about additional matches
				continue OUTER
			}
		}
	}
	f.score = score
}
