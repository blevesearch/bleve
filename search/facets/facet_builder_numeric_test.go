package facets

import (
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/index"
	nu "github.com/blevesearch/bleve/numeric_util"
)

var pcodedvalues []nu.PrefixCoded

func init() {
	pcodedvalues = []nu.PrefixCoded{{0x20, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}, {0x20, 0x0, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f}, {0x20, 0x0, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7a, 0x1d, 0xa}, {0x20, 0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x16, 0x9, 0x4a, 0x7b}}
}

func BenchmarkNumericFacet10(b *testing.B) {
	numericFacetN(b, 10)
}

func BenchmarkNumericFacet100(b *testing.B) {
	numericFacetN(b, 100)
}

func BenchmarkNumericFacet1000(b *testing.B) {
	numericFacetN(b, 1000)
}

func numericFacetN(b *testing.B, numTerms int) {
	field := "test"
	nfb := NewNumericFacetBuilder(field, numTerms)
	min, max := 0.0, 9999999998.0

	for i := 0; i <= numTerms; i++ {
		max++
		min--

		nfb.AddRange("rangename"+strconv.Itoa(i), &min, &max)

		for _, pv := range pcodedvalues {
			nfb.Update(index.FieldTerms{field: []string{string(pv)}})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nfb.Result()
	}
}
