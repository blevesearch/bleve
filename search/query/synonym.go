package query

import (
	"context"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

const defaultBoost float64 = 1.0
const defaultFuzziness = 0
const defaultPrefix = 0
const defaultOperator = 0

type SynonymQuery struct {
	Graph        [][]*analysis.Token `json:"graph"`
	FieldVal     string              `json:"field,omitempty"`
	BoostVal     *Boost              `json:"boost,omitempty"`
	FuzzinessVal int                 `json:"fuzziness,omitempty"`
	PrefixVal    int                 `json:"prefix,omitempty"`
	OperatorVal  int                 `json:"operator,omitempty"`
}

func NewSynonymQuery(graph [][]*analysis.Token, field string) *SynonymQuery {
	defaultBoost := Boost(defaultBoost)
	return &SynonymQuery{
		Graph:        graph,
		FieldVal:     field,
		BoostVal:     &defaultBoost,
		FuzzinessVal: defaultFuzziness,
		PrefixVal:    defaultPrefix,
		OperatorVal:  defaultOperator,
	}
}

func (q *SynonymQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *SynonymQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *SynonymQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *SynonymQuery) Field() string {
	return q.FieldVal
}

func (q *SynonymQuery) SetFuzziness(f int) {
	q.FuzzinessVal = f
}

func (q *SynonymQuery) Fuzziness() int {
	return q.FuzzinessVal
}

func (q *SynonymQuery) SetOperator(f int) {
	q.OperatorVal = f
}

func (q *SynonymQuery) Operator() int {
	return q.OperatorVal
}

func (q *SynonymQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	return searcher.NewSynonymSearcher(ctx, i, q.Graph, field, q.BoostVal.Value(), q.FuzzinessVal, q.PrefixVal, q.OperatorVal, options)
}
