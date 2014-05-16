package upside_down

import (
	"os"
	"testing"

	"github.com/couchbaselabs/bleve/index/store/leveldb"
)

func BenchmarkLevelDBIndexing(b *testing.B) {
	s, err := leveldb.Open("test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s)
}
