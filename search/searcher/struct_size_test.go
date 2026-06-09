package searcher

import (
	"testing"
	"unsafe"
)

// TestDSSStructSize guards against accidental growth of DisjunctionSliceSearcher.
// The struct must remain exactly 384 bytes (6 × 64-byte cache lines) for the
// hot-field / cold-field cache line layout to stay correct. Hot fields used in
// nextMAXSCORE's inner loop (numSearchers, lazyMode, currs) live on cache line 1
// (offsets 64–127); cold fields (lazySearchers) live on cache line 5 (320–383).
// Any addition that pushes the total past 384 bytes creates a 7th cache line and
// can cause a measurable regression (~9%) on k=1000 queries.
func TestDSSStructSize(t *testing.T) {
	var s DisjunctionSliceSearcher
	size := unsafe.Sizeof(s)
	if size != 384 {
		t.Errorf("DisjunctionSliceSearcher size = %d bytes, want 384 (6 × 64-byte cache lines); "+
			"adding a field beyond the existing 7-byte padding in cache line 1 will create a "+
			"7th cache line and regress k=1000 benchmarks by ~9%%", size)
	}
}
