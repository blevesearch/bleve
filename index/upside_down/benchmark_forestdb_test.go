// +build forestdb

package upside_down

import (
	"os"
	"testing"

	"github.com/couchbaselabs/bleve/index/store/goforestdb"
)

func BenchmarkForestDBIndexing(b *testing.B) {
	s, err := goforestdb.Open("test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s)
}
