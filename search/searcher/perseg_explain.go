//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// The explanation of a match is built from the bottom: a term looks its doc up in
// its postings and has its scorer explain the posting; a composite explains each
// of its clauses and puts them together the way it puts their scores together
// when it scores, in the same order and with the same float32 arithmetic, so that
// the score at its root is the score of the hit to the bit. It is the same sums,
// done again for the few docs that were returned. See search.PerSegmentExplainer.

var (
	_ search.PerSegmentExplainer = (*PerSegmentTermSearcher)(nil)
	_ search.PerSegmentExplainer = (*PerSegmentConjunctionSearcher)(nil)
	_ search.PerSegmentExplainer = (*PerSegmentDisjunctionSearcher)(nil)
	_ search.PerSegmentExplainer = (*PerSegmentBooleanSearcher)(nil)
)

// clausesByCount is the clauses of a composite in the order a regular searcher
// lists them: by the number of docs the clause has, the fewest first, by the very
// sort the regular searchers use (so that clauses of the same count end up where
// they do there). The sums are not in this order, they are in the order of the
// query; it is only the order of an explanation.
type clausesByCount struct {
	clauses []perSegChild
	pos     []int // where each clause is in the query
}

func (c clausesByCount) Len() int { return len(c.clauses) }
func (c clausesByCount) Less(i, j int) bool {
	return regularCount(c.clauses[i]) < regularCount(c.clauses[j])
}
func (c clausesByCount) Swap(i, j int) {
	c.clauses[i], c.clauses[j] = c.clauses[j], c.clauses[i]
	c.pos[i], c.pos[j] = c.pos[j], c.pos[i]
}

// regularCount is the Count of the regular searcher of the clause, which is what
// the regular searchers sort their clauses by: the worst case, the sum of the
// counts of the clauses of a composite (a per segment conjunction reports the
// least instead).
func regularCount(c perSegChild) uint64 {
	var sum uint64
	switch s := c.(type) {
	case *PerSegmentConjunctionSearcher:
		for _, child := range s.children {
			sum += regularCount(child)
		}
	case *PerSegmentDisjunctionSearcher:
		for _, child := range s.children {
			sum += regularCount(child)
		}
	case *PerSegmentBooleanSearcher:
		if s.must != nil {
			sum += regularCount(s.must)
		}
		if s.should != nil {
			sum += regularCount(s.should)
		}
	default:
		return c.Count()
	}
	return sum
}

// regularOrder is where, in the query, the clauses are, in the order the regular
// searchers list them.
func regularOrder(clauses []perSegChild) []int {
	byCount := clausesByCount{clauses: append([]perSegChild(nil), clauses...), pos: make([]int, len(clauses))}
	for i := range byCount.pos {
		byCount.pos[i] = i
	}
	sort.Sort(byCount)
	return byCount.pos
}

// explainClause explains a match of a clause of a composite, wrapped in what it
// was wrapped in before it was unwrapped, innermost first.
func explainClause(c perSegChild, wraps []wrapKind, seg int, doc uint64) (*search.Explanation, bool, error) {
	ex, ok := c.(search.PerSegmentExplainer)
	if !ok {
		return nil, false, fmt.Errorf("searcher: %T can't explain its matches", c)
	}
	e, match, err := ex.ExplainMatch(seg, doc)
	if err != nil || !match {
		return e, match, err
	}
	for i := len(wraps) - 1; i >= 0; i-- {
		switch wraps[i] {
		case wrapConjunction:
			// the sum of one score
			e = &search.Explanation{Value: e.Value, Message: "sum of:", Children: []*search.Explanation{e}}
		case wrapDisjunction:
			// the sum of one score times a coord of 1 of 1
			raw := &search.Explanation{Value: e.Value, Message: "sum of:", Children: []*search.Explanation{e}}
			e = &search.Explanation{
				Value:   e.Value,
				Message: "product of:",
				Children: []*search.Explanation{raw,
					{Value: 1, Message: "coord(1/1)"}},
			}
		}
	}
	return e, true, nil
}

// wrapsOf is what the clause i of a composite was wrapped in.
func (b *perSegBase) wrapsOf(i int) []wrapKind {
	if i < len(b.wraps) {
		return b.wraps[i]
	}
	return nil
}

// ExplainMatch implements search.PerSegmentExplainer.
func (s *PerSegmentTermSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	if seg < 0 || seg >= len(s.readers) || s.readers[seg] == nil {
		return nil, false, nil
	}
	r := s.readers[seg]
	if doc < r.Offset() || doc-r.Offset() > uint64(^uint32(0)) {
		return nil, false, nil
	}
	freq, norm, ok, err := r.Probe(uint32(doc - r.Offset()))
	if err != nil || !ok {
		return nil, false, err
	}
	id := index.NewIndexInternalID(nil, doc)
	return s.scorer.Explain(s.field, s.term, id, freq, norm), true, nil
}

// ExplainMatch implements search.PerSegmentExplainer. A conjunction's score is
// the sum of its clauses' scores.
func (s *PerSegmentConjunctionSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	expls := make([]*search.Explanation, len(s.children))
	var sum float32
	for i, c := range s.children {
		e, ok, err := explainClause(c, s.wrapsOf(i), seg, doc)
		if err != nil || !ok {
			return nil, false, err
		}
		expls[i] = e
		sum += float32(e.Value)
	}
	children := make([]*search.Explanation, len(expls))
	for i, pos := range regularOrder(s.children) {
		children[i] = expls[pos]
	}
	return &search.Explanation{Value: float64(sum), Message: "sum of:", Children: children}, true, nil
}

// ExplainMatch implements search.PerSegmentExplainer. A disjunction's score is
// the sum of the scores of the clauses that match times the share of the clauses
// that do (coord).
func (s *PerSegmentDisjunctionSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	expls := make([]*search.Explanation, len(s.children)) // nil where the clause doesn't match
	matches := 0
	var sum float32
	for i, c := range s.children {
		e, ok, err := explainClause(c, s.wrapsOf(i), seg, doc)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		expls[i] = e
		matches++
		sum += float32(e.Value)
	}
	if matches == 0 || matches < s.min {
		return nil, false, nil
	}
	matched := make([]*search.Explanation, 0, matches)
	if len(s.children) <= DisjunctionHeapTakeover {
		// a regular disjunction of few clauses lists them by count; of many it is
		// a heap, which lists them in the order it finds them
		for _, pos := range regularOrder(s.children) {
			if expls[pos] != nil {
				matched = append(matched, expls[pos])
			}
		}
	} else {
		for _, e := range expls {
			if e != nil {
				matched = append(matched, e)
			}
		}
	}
	n := len(s.children)
	coord := float32(len(matched)) / float32(n)
	raw := &search.Explanation{Value: float64(sum), Message: "sum of:", Children: matched}
	return &search.Explanation{
		Value:   float64(sum * coord),
		Message: "product of:",
		Children: []*search.Explanation{
			raw,
			{Value: float64(coord), Message: fmt.Sprintf("coord(%d/%d)", len(matched), n)},
		},
		PartialMatch: len(matched) != n,
	}, true, nil
}

// ExplainMatch implements search.PerSegmentExplainer. A boolean's score is the
// sum of the scores of its required clause and, where it matches, its optional
// one.
func (s *PerSegmentBooleanSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	var children []*search.Explanation
	var sum float32
	if s.must != nil {
		e, ok, err := explainClause(s.must, s.mustWraps, seg, doc)
		if err != nil || !ok {
			return nil, false, err
		}
		children = append(children, e)
		sum += float32(e.Value)
	}
	if s.mustNot != nil {
		_, excluded, err := explainClause(s.mustNot, s.mustNotWraps, seg, doc)
		if err != nil || excluded {
			return nil, false, err
		}
	}
	if s.should != nil {
		e, ok, err := explainClause(s.should, s.shouldWraps, seg, doc)
		if err != nil {
			return nil, false, err
		}
		switch {
		case ok:
			children = append(children, e)
			sum += float32(e.Value)
		case s.must == nil || s.shouldRequired:
			return nil, false, nil
		}
	}
	if len(children) == 0 {
		return nil, false, nil
	}
	return &search.Explanation{Value: float64(sum), Message: "sum of:", Children: children}, true, nil
}
