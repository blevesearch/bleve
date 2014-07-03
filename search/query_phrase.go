package search

import (
	"fmt"

	"github.com/couchbaselabs/bleve/index"
)

type PhraseQuery struct {
	Terms           []*TermQuery       `json:"terms,omitempty"`
	PhrasePositions map[string]float64 `json:"phrase_positions,omitempty"`
	BoostVal        float64            `json:"boost,omitempty"`
	Explain         bool               `json:"explain,omitempty"`
}

func (q *PhraseQuery) Boost() float64 {
	return q.BoostVal
}

func (q *PhraseQuery) Searcher(index index.Index) (Searcher, error) {
	return NewPhraseSearcher(index, q)
}

func (q *PhraseQuery) Validate() error {
	if q.Terms == nil {
		return fmt.Errorf("Phrase query must contain at least one term")
	}
	return nil
}
