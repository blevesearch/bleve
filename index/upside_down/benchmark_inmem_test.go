package upside_down

import (
	"testing"

	"github.com/couchbaselabs/bleve/index/store/inmem"
)

func BenchmarkInMemIndexing(b *testing.B) {
	s, err := inmem.Open()
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	CommonBenchmarkIndex(b, s)
}
