package scorch

import (
	"fmt"

	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// verifyMergeResult reads every posting of every term of every field
// in the segment to detect encoding errors (e.g. corrupt varint data).
// Returns nil if all postings are readable, or the first error found.
//
// This is a defense-in-depth measure against merge encoding bugs that
// produce segments with corrupt freq/norm data. By verifying before
// introduction, corrupt segments are discarded and source segments
// preserved, preventing permanent index corruption.
func verifyMergeResult(seg segment.Segment) error {
	fields := seg.Fields()
	for _, field := range fields {
		dict, err := seg.Dictionary(field)
		if err != nil {
			return fmt.Errorf("field %s: Dictionary: %v", field, err)
		}
		itr := dict.AutomatonIterator(nil, nil, nil)
		for {
			entry, err := itr.Next()
			if err != nil {
				return fmt.Errorf("field %s: dict iterator: %v", field, err)
			}
			if entry == nil {
				break
			}
			pl, err := dict.PostingsList([]byte(entry.Term), nil, nil)
			if err != nil {
				t := entry.Term
				if len(t) > 80 {
					t = t[:80] + "..."
				}
				return fmt.Errorf("field %s term %q: PostingsList: %v", field, t, err)
			}
			pItr := pl.Iterator(true, true, true, nil)
			count := 0
			for {
				p, err := pItr.Next()
				if err != nil {
					t := entry.Term
					if len(t) > 80 {
						t = t[:80] + "..."
					}
					return fmt.Errorf("field %s term %q posting %d: %v", field, t, count, err)
				}
				if p == nil {
					break
				}
				count++
			}
		}
	}
	return nil
}
