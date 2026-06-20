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
//
//	384 bytes (6 cache lines) — original
//	408 bytes — currIDs []uint64 cache (24 bytes: slice header); eliminates BigEndian
//	            decode + pointer chase in nextMAXSCORE collect/advance loops
//
// §7 and §35 land later on this branch and grow this further (to 456, 464 and 488
// respectively in the original ordering); their commits carry those updates.
func TestDSSStructSize(t *testing.T) {
	var s DisjunctionSliceSearcher
	size := unsafe.Sizeof(s)
	if size != 408 {
		t.Errorf("DisjunctionSliceSearcher size = %d bytes, want 408; "+
			"update this test and the struct comment if you intentionally resized it", size)
	}
}
