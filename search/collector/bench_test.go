package collectors

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"golang.org/x/net/context"
)

type createCollector func() search.Collector

func benchHelper(numOfMatches int, cc createCollector, b *testing.B) {
	matches := make([]*search.DocumentMatch, 0, numOfMatches)
	for i := 0; i < numOfMatches; i++ {
		matches = append(matches, &search.DocumentMatch{
			IndexInternalID: index.IndexInternalID(strconv.Itoa(i)),
			Score:           rand.Float64(),
		})
	}

	b.ResetTimer()

	for run := 0; run < b.N; run++ {
		searcher := &stubSearcher{
			matches: matches,
		}
		collector := cc()
		err := collector.Collect(context.Background(), searcher, &stubReader{})
		if err != nil {
			b.Fatal(err)
		}
	}
}
