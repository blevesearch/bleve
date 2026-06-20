package searcher

import (
	"testing"
	"unsafe"
)

// TestDSSStructSize guards against accidental growth of DisjunctionSliceSearcher.
// Hot fields used in nextMAXSCORE's inner loop (numSearchers, lazyMode, currs)
// remain on cache lines 0–1 (offsets 0–127).
//
// Size history:
//   384 bytes (6 cache lines) — original
//   456 bytes — §7 added options/ctx/parallelResults/parallelPos (cold, end of struct)
//   464 bytes — §35 added TopK int to SearcherOptions (stored in options field)
//   488 bytes — currIDs []uint64 cache (24 bytes: slice header); eliminates BigEndian
//               decode + pointer chase in nextMAXSCORE collect/advance loops
func TestDSSStructSize(t *testing.T) {
	var s DisjunctionSliceSearcher
	size := unsafe.Sizeof(s)
	if size != 488 {
		t.Errorf("DisjunctionSliceSearcher size = %d bytes, want 488; "+
			"update this test and the struct comment if you intentionally resized it", size)
	}
}
