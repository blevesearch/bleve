package facets

import (
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/blevesearch/bleve/index"
)

var terms []string

func init() {
	wsRegexp := regexp.MustCompile(`\W+`)
	input, err := ioutil.ReadFile("benchmark_data.txt")
	if err != nil {
		panic(err)
	}
	terms = wsRegexp.Split(string(input), -1)
}

func BenchmarkTermsFacet10(b *testing.B) {
	termsFacetN(b, 10)
}

func BenchmarkTermsFacet100(b *testing.B) {
	termsFacetN(b, 100)
}

func BenchmarkTermsFacet1000(b *testing.B) {
	termsFacetN(b, 1000)
}

func BenchmarkTermsFacet10000(b *testing.B) {
	termsFacetN(b, 10000)
}

// func BenchmarkTermsFacet100000(b *testing.B) {
// 	termsFacetN(b, 100000)
// }

func termsFacetN(b *testing.B, numTerms int) {
	field := "test"
	termsLen := len(terms)
	tfb := NewTermsFacetBuilder(field, 3)
	i := 0
	for len(tfb.termsCount) < numTerms && i <= termsLen {
		j := i % termsLen
		term := terms[j]
		tfb.Update(index.FieldTerms{field: []string{term}})
		i++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tfb.Result()
	}
}
