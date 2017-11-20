package query

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
)

type IPRangeQuery struct {
	CIDRVal  string `json:"cidr", omitempty`
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
	_, ipNet, err := net.ParseCIDR(q.CIDRVal)
	if err != nil {
		isIP := net.ParseIP(q.CIDRVal)
		if isIP == nil {
			return nil, err
		}

		if isIP.DefaultMask() != nil {
			q.CIDRVal = q.CIDRVal + `/32`
		} else {
			q.CIDRVal = q.CIDRVal + `/128`
		}
		_, ipNet, err = net.ParseCIDR(q.CIDRVal)
		if err != nil {
			return nil, err
		}
	}
	cq := ipRangeToConjuctionQuery(q.FieldVal, ipNet)
	return cq.Searcher(i, m, options)
}

func (q *IPRangeQuery) Validate() error {
	return nil
}

func ipRangeToConjuctionQuery(fielName string, ipNet *net.IPNet) *ConjunctionQuery {
	bndrs := make([]struct {
		min uint32
		max uint32
	}, 4)
	isIPv4 := false
	if ipNet.IP.DefaultMask() != nil {
		isIPv4 = true
	}

	if ok4 := ipNet.IP.To4(); ok4 != nil {
		bndrs[2].min = 0x0000FFFF
		bndrs[2].max = 0x0000FFFF
		bndrs[3].min = binary.BigEndian.Uint32(ok4)

		a := make([]byte, 4)
		m := ipNet.Mask
		for i, b := range ok4 {
			// calculate broadcast address (end of range)
			a[i] = b | ^m[i]
		}
		bndrs[3].max = binary.BigEndian.Uint32(a)
	} else {
		// IPv6
		for i, _ := range bndrs {
			chunk := ipNet.IP[i*4 : (i+1)*4]
			bndrs[i].min = binary.BigEndian.Uint32(chunk)

			t := make([]byte, 4)
			mask := ipNet.Mask[i*4 : (i+1)*4]
			for k, b := range chunk {
				t[k] = b | ^mask[k]
			}
			bndrs[i].max = binary.BigEndian.Uint32(t)
		}

	}

	qs := make([]Query, 4)
	for i, bs := range bndrs {
		tq := newNumericRangeInclusiveQuery(bs.min, bs.max, true, true)
		tq.SetField(fmt.Sprintf("%s_IP%d", fielName, i+1))
		qs[i] = tq
	}

	cq := NewConjunctionQuery(qs)

	blq := NewBoolFieldQuery(isIPv4)
	blq.SetField(fielName + "_isIPv4")
	cq.AddQuery(blq)

	return cq
}

func newNumericRangeInclusiveQuery(min, max uint32, minInclusive, maxInclusive bool) *NumericRangeQuery {
	m1 := float64(min)
	m2 := float64(max)
	return NewNumericRangeInclusiveQuery(&m1, &m2, &minInclusive, &maxInclusive)
}
