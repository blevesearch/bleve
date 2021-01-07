package cn

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "cn"
type ScwsAnalyzer struct {
}

func analyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {
	tokenizer, err := cache.TokenizerNamed(Name)
	if err != nil {
		return nil, err
	}
	alz := &analysis.Analyzer{
		Tokenizer: tokenizer,
	}
	return alz, nil
}

func init() {
	registry.RegisterAnalyzer(Name, analyzerConstructor)
}
