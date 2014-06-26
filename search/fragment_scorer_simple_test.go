package search

import (
	"testing"
)

func TestSimpleFragmentScorer(t *testing.T) {

	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		score    float64
	}{
		{
			fragment: &Fragment{
				orig:  []byte("cat in the hat"),
				start: 0,
				end:   14,
			},
			tlm: TermLocationMap{
				"cat": Locations{
					&Location{
						Pos:   0,
						Start: 0,
						End:   3,
					},
				},
			},
			score: 1,
		},
		{
			fragment: &Fragment{
				orig:  []byte("cat in the hat"),
				start: 0,
				end:   14,
			},
			tlm: TermLocationMap{
				"cat": Locations{
					&Location{
						Pos:   1,
						Start: 0,
						End:   3,
					},
				},
				"hat": Locations{
					&Location{
						Pos:   4,
						Start: 11,
						End:   14,
					},
				},
			},
			score: 2,
		},
	}

	for _, test := range tests {
		scorer := NewSimpleFragmentScorer(test.tlm)
		scorer.Score(test.fragment)
		if test.fragment.score != test.score {
			t.Errorf("expected score %f, got %f", test.score, test.fragment.score)
		}
	}

}
