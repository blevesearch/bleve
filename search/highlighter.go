package search

import (
	"github.com/couchbaselabs/bleve/document"
)

type Fragment struct {
	orig  []byte
	start int
	end   int
	score float64
	index int // used by heap
}

func (f *Fragment) Overlaps(other *Fragment) bool {
	if other.start >= f.start && other.start < f.end {
		return true
	}
	return false
}

type Fragmenter interface {
	Fragment([]byte, termLocations) []*Fragment
}

type FragmentFormatter interface {
	Format(f *Fragment, tlm TermLocationMap) string
}

type FragmentScorer interface {
	Score(f *Fragment) float64
}

type Highlighter interface {
	Fragmenter() Fragmenter
	SetFragmenter(Fragmenter)

	FragmentFormatter() FragmentFormatter
	SetFragmentFormatter(FragmentFormatter)

	Separator() string
	SetSeparator(string)

	BestFragmentInField(*DocumentMatch, *document.Document, string) string
	BestFragmentsInField(*DocumentMatch, *document.Document, string, int) []string
}
