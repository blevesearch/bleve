package synonymFilter

import (
	"fmt"
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "synonymFilter"

type SynonymFilter struct {
	synonymMap *[]analysis.SynonymStruct
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for tokenIndex, token := range input {
		fmt.Println(tokenIndex, token)
	}
	for synonymID, synonym := range *(s.synonymMap) {
		fmt.Print("SYNONYM NUMBER\t")
		fmt.Println(synonymID)
		fmt.Println("LHS")
		for _, LHS := range synonym.LHS {
			fmt.Print("\t")
			fmt.Println(string(LHS))
		}
		fmt.Println()
		fmt.Println("RHS")
		for _, RHS := range synonym.RHS {
			fmt.Print("\t")
			fmt.Println(string(RHS))
		}
		fmt.Println()
		fmt.Println("Bidirectional\t", synonym.BiDirectional)
		fmt.Println("KeepOrig\t", synonym.KeepOrig)
	}
	return input
}

func NewSynonymFilter(synonymMap *[]analysis.SynonymStruct) *SynonymFilter {
	return &SynonymFilter{
		synonymMap: synonymMap,
	}
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	synonymMap, ok := config["synonymMap"].(*[]analysis.SynonymStruct)
	if !ok {
		return nil, fmt.Errorf("must specify synonym map")
	}
	return NewSynonymFilter(synonymMap), nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
