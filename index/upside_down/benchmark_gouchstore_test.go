package upside_down

import (
	"os"
	"testing"

	"github.com/couchbaselabs/bleve/index/store/gouchstore"
)

func BenchmarkGouchstoreIndexing(b *testing.B) {
	s, err := gouchstore.Open("test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s)
}
