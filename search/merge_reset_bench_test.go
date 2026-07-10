package search

import "testing"

// BenchmarkMergeFieldTermLocationsNoLocs: common no-highlighting case — several
// constituent matches, none carrying field term locations. Exercises the
// fast-path early return.
func BenchmarkMergeFieldTermLocationsNoLocs(b *testing.B) {
	matches := make([]*DocumentMatch, 5)
	for i := range matches {
		matches[i] = &DocumentMatch{}
	}
	b.ResetTimer()
	b.ReportAllocs()
	var dest []FieldTermLocation
	for n := 0; n < b.N; n++ {
		dest = MergeFieldTermLocations(nil, matches)
	}
	_ = dest
}

// BenchmarkDocumentMatchResetNilScoreBreakdown: Reset() on the common path where
// ScoreBreakdown is nil. Exercises the clear(scoreBreakdown) nil-guard.
func BenchmarkDocumentMatchResetNilScoreBreakdown(b *testing.B) {
	dm := &DocumentMatch{}
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		dm.Reset()
	}
}
