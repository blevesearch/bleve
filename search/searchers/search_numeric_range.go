//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"bytes"
	"math"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/numeric_util"
	"github.com/blevesearch/bleve/search"
)

type NumericRangeSearcher struct {
	indexReader index.IndexReader
	min         *float64
	max         *float64
	field       string
	explain     bool
	searcher    *DisjunctionSearcher
}

func NewNumericRangeSearcher(indexReader index.IndexReader, min *float64, max *float64, inclusiveMin, inclusiveMax *bool, field string, boost float64, explain bool) (*NumericRangeSearcher, error) {
	// account for unbounded edges
	if min == nil {
		negInf := math.Inf(-1)
		min = &negInf
	}
	if max == nil {
		Inf := math.Inf(1)
		max = &Inf
	}
	if inclusiveMin == nil {
		defaultInclusiveMin := true
		inclusiveMin = &defaultInclusiveMin
	}
	if inclusiveMax == nil {
		defaultInclusiveMax := false
		inclusiveMax = &defaultInclusiveMax
	}
	// find all the ranges
	minInt64 := numeric_util.Float64ToInt64(*min)
	if !*inclusiveMin && minInt64 != math.MaxInt64 {
		minInt64++
	}
	maxInt64 := numeric_util.Float64ToInt64(*max)
	if !*inclusiveMax && maxInt64 != math.MinInt64 {
		maxInt64--
	}
	// FIXME hard-coded precision, should match field declaration
	termRanges := splitInt64Range(minInt64, maxInt64, 4)
	terms := termRanges.Enumerate()
	if tooManyClauses(len(terms)) {
		return nil, tooManyClausesErr()
	}
	// enumerate all the terms in the range
	qsearchers := make([]search.Searcher, len(terms))
	for i, term := range terms {
		var err error
		qsearchers[i], err = NewTermSearcher(indexReader, string(term), field, boost, explain)
		if err != nil {
			return nil, err
		}
	}
	// build disjunction searcher of these ranges
	searcher, err := NewDisjunctionSearcher(indexReader, qsearchers, 0, explain)
	if err != nil {
		return nil, err
	}
	return &NumericRangeSearcher{
		indexReader: indexReader,
		min:         min,
		max:         max,
		field:       field,
		explain:     explain,
		searcher:    searcher,
	}, nil
}

func (s *NumericRangeSearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *NumericRangeSearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *NumericRangeSearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *NumericRangeSearcher) Next(preAllocated *search.DocumentMatch) (*search.DocumentMatch, error) {
	return s.searcher.Next(preAllocated)
}

func (s *NumericRangeSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return s.searcher.Advance(ID)
}

func (s *NumericRangeSearcher) Close() error {
	return s.searcher.Close()
}

type termRange struct {
	startTerm []byte
	endTerm   []byte
}

func (t *termRange) Enumerate() [][]byte {
	rv := make([][]byte, 0)
	next := t.startTerm
	for bytes.Compare(next, t.endTerm) <= 0 {
		rv = append(rv, next)
		next = incrementBytes(next)
	}
	return rv
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			// didn't overflow, so stop
			break
		}
	}
	return rv
}

type termRanges []*termRange

func (tr termRanges) Enumerate() [][]byte {
	rv := make([][]byte, 0)
	for _, tri := range tr {
		trie := tri.Enumerate()
		rv = append(rv, trie...)
	}
	return rv
}

func splitInt64Range(minBound, maxBound int64, precisionStep uint) termRanges {
	rv := make(termRanges, 0)
	if minBound > maxBound {
		return rv
	}

	for shift := uint(0); ; shift += precisionStep {

		diff := int64(1) << (shift + precisionStep)
		mask := ((int64(1) << precisionStep) - int64(1)) << shift
		hasLower := (minBound & mask) != int64(0)
		hasUpper := (maxBound & mask) != mask

		var nextMinBound int64
		if hasLower {
			nextMinBound = (minBound + diff) &^ mask
		} else {
			nextMinBound = minBound &^ mask
		}
		var nextMaxBound int64
		if hasUpper {
			nextMaxBound = (maxBound - diff) &^ mask
		} else {
			nextMaxBound = maxBound &^ mask
		}

		lowerWrapped := nextMinBound < minBound
		upperWrapped := nextMaxBound > maxBound

		if shift+precisionStep >= 64 || nextMinBound > nextMaxBound || lowerWrapped || upperWrapped {
			// We are in the lowest precision or the next precision is not available.
			rv = append(rv, newRange(minBound, maxBound, shift))
			// exit the split recursion loop
			break
		}

		if hasLower {
			rv = append(rv, newRange(minBound, minBound|mask, shift))
		}
		if hasUpper {
			rv = append(rv, newRange(maxBound&^mask, maxBound, shift))
		}

		// recurse to next precision
		minBound = nextMinBound
		maxBound = nextMaxBound
	}

	return rv
}

func newRange(minBound, maxBound int64, shift uint) *termRange {
	maxBound |= (int64(1) << shift) - int64(1)
	minBytes := numeric_util.MustNewPrefixCodedInt64(minBound, shift)
	maxBytes := numeric_util.MustNewPrefixCodedInt64(maxBound, shift)
	return newRangeBytes(minBytes, maxBytes)
}

func newRangeBytes(minBytes, maxBytes []byte) *termRange {
	return &termRange{
		startTerm: minBytes,
		endTerm:   maxBytes,
	}
}

func (s *NumericRangeSearcher) Min() int {
	return 0
}
