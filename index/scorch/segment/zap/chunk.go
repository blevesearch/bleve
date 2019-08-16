package zap

import "fmt"

func getChunkSize(chunkMode uint32, cardinality uint64, maxDocs uint64) (uint64, error) {
	switch chunkMode {
	case 1024:
		// legacy chunk size
		return 1024, nil
	case 1:
		return 1, nil
	case 0:
		// attempt at simple improvement
		// theory - the point of chunking is to put a bound on the maximum number of
		// calls to Next() needed to find a random document.  ie, you should be able
		// to do one jump to the correct chunk, and then walk through at most
		// chunk-size items
		// previously 1024 was chosen as the chunk size, but this is particularly
		// wasteful for low cardinality terms.  the observation is that if there
		// are less than 1024 items, why not put them all in one chunk,
		// this way you'll still achieve the same goal of visiting at most
		// chunk-size items.
		// no attempt is made to tweak any other case
		if cardinality < 1024 {
			return maxDocs, nil
		}
		return 1024, nil
	}
	return 0, fmt.Errorf("unknown chunk mode %d", chunkMode)
}
