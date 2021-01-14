package query

import (
	"net"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type IPRangeQuery struct {
	CIDRVal  string `json:"cidr, omitempty"`
	FieldVal string `json:"field,omitempty"`
	BoostVal *Boost `json:"boost,omitempty"`
}

func NewIPRangeQuery(cidr string) *IPRangeQuery {
	return &IPRangeQuery{
		CIDRVal: cidr,
	}
}

func (q *IPRangeQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *IPRangeQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *IPRangeQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *IPRangeQuery) Field() string {
	return q.FieldVal
}

func (q *IPRangeQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	_, ipNet, err := net.ParseCIDR(q.CIDRVal)
	if err != nil {
		ip := net.ParseIP(q.CIDRVal)
		if ip == nil {
			return nil, err
		}
		// If we are searching for a specific ip rather than members of a network, just use a term search.
		return searcher.NewTermSearcherBytes(i, ip.To16(), field, q.BoostVal.Value(), options)
	}
	return searcher.NewIpRangeSearcher(i, ipNet, field, q.BoostVal.Value(), options)
}

func (q *IPRangeQuery) Validate() error {
	_, _, err := net.ParseCIDR(q.CIDRVal)
	return err
}
