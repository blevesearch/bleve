package cn

import (
	"errors"
	"runtime"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

type ScwsTokenizer struct {
	handle *Scws
}
const Name = "cn"
var(
	Dict string
	Rule string
)
func SetDict(dict string){
	Dict = dict
}
func SetRule(rule string){
	Rule = rule
}

func NewScwsTokenizer() (*ScwsTokenizer,error){
	if Dict=="" {
		return nil, errors.New("config dict file not found")
	}
	x := NewScws()
	err := x.SetDict(Dict, SCWS_XDICT_XDB)
	if err != nil {
		return  nil,err
	}
	if Rule != "" {
		err = x.SetRule(Rule)
		if err != nil {
			return nil ,err
		}
	}

	x.SetCharset("utf8")
	x.SetIgnore(1)
	x.SetMulti(SCWS_MULTI_SHORT | SCWS_MULTI_DUALITY)

	x.Init(runtime.NumCPU())
	return &ScwsTokenizer{x},nil
}

func (x *ScwsTokenizer) Free() {
	x.handle.Free()
}

func (x *ScwsTokenizer) Tokenize(sentence []byte) analysis.TokenStream {
	result := make(analysis.TokenStream, 0)
	pos := 1
	words, err := x.handle.Segment(string(sentence))
	if err != nil {
		panic(err)
		return nil

	}
	for _, word := range words {
		token := analysis.Token{
			Term:     []byte(word.Term),
			Start:    word.Start,
			End:      word.End,
			Position: pos,
			Type:     analysis.Ideographic,
		}
		result = append(result, &token)
		pos++
	}
	return result
}

func tokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	dict, ok := config["dict"].(string)
	if ok {
		SetDict(dict);
	}
	rule, ok := config["rule"].(string)
	if ok {
		SetRule(rule);
	}

	scws,err := NewScwsTokenizer()
	if(err !=nil){
		return nil,err
	}
	return scws,nil
}

func init() {
	registry.RegisterTokenizer(Name, tokenizerConstructor)
}
