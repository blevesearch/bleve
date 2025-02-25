package hierarchy

import (
	"bytes"
	"fmt"
	"math"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "hierarchy"

type HierarchyFilter struct {
	maxLevels  int
	delimiter  []byte
	splitInput bool
}

func NewHierarchyFilter(delimiter []byte, maxLevels int, splitInput bool) *HierarchyFilter {
	return &HierarchyFilter{
		maxLevels:  maxLevels,
		delimiter:  delimiter,
		splitInput: splitInput,
	}
}

func (s *HierarchyFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0, s.maxLevels)

	var soFar [][]byte
	for _, token := range input {
		if s.splitInput {
			parts := bytes.Split(token.Term, s.delimiter)
			for _, part := range parts {
				soFar, rv = s.buildToken(rv, soFar, part)
				if len(soFar) >= s.maxLevels {
					return rv
				}
			}
		} else {
			soFar, rv = s.buildToken(rv, soFar, token.Term)
			if len(soFar) >= s.maxLevels {
				return rv
			}
		}
	}

	return rv
}

func (s *HierarchyFilter) buildToken(tokenStream analysis.TokenStream, soFar [][]byte, part []byte) (
	[][]byte, analysis.TokenStream) {

	soFar = append(soFar, part)
	term := bytes.Join(soFar, s.delimiter)

	tokenStream = append(tokenStream, &analysis.Token{
		Type:     analysis.Shingle,
		Term:     term,
		Start:    0,
		End:      len(term),
		Position: 1,
	})

	return soFar, tokenStream
}

func HierarchyFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	max := math.MaxInt64
	maxVal, ok := config["max"].(float64)
	if ok {
		max = int(maxVal)
	}

	splitInput := true
	splitInputVal, ok := config["split_input"].(bool)
	if ok {
		splitInput = splitInputVal
	}

	delimiter, ok := config["delimiter"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify delimiter")
	}

	return NewHierarchyFilter([]byte(delimiter), max, splitInput), nil
}

func init() {
	err := registry.RegisterTokenFilter(Name, HierarchyFilterConstructor)
	if err != nil {
		panic(err)
	}
}
