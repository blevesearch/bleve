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
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizePerSegmentMatchNoneSearcher int

func init() {
	var s PerSegmentMatchNoneSearcher
	reflectStaticSizePerSegmentMatchNoneSearcher = int(reflect.TypeOf(s).Size())
}

// PerSegmentMatchNoneSearcher is the per segment searcher of a query that matches
// nothing: a match of a text that analyzes to no tokens, say. It can be a clause
// of a composite, which is what it is for; the queries that build the searchers
// of the composites leave it out or give a composite of nothing but it, like the
// regular searchers do for MatchNoneSearcher.
type PerSegmentMatchNoneSearcher struct{}

var _ search.PerSegmentSearcher = (*PerSegmentMatchNoneSearcher)(nil)
var _ search.PerSegmentExplainer = (*PerSegmentMatchNoneSearcher)(nil)
var _ perSegChild = (*PerSegmentMatchNoneSearcher)(nil)

// NewPerSegmentMatchNoneSearcher returns a searcher that has no matches.
func NewPerSegmentMatchNoneSearcher() *PerSegmentMatchNoneSearcher {
	return &PerSegmentMatchNoneSearcher{}
}

// NextMatch implements search.PerSegmentSearcher.
func (s *PerSegmentMatchNoneSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	return search.PerSegmentMatch{}, false, nil
}

func (s *PerSegmentMatchNoneSearcher) Close() error { return nil }

func (s *PerSegmentMatchNoneSearcher) Weight() float64 { return 0 }

func (s *PerSegmentMatchNoneSearcher) SetQueryNorm(float64) {}

func (s *PerSegmentMatchNoneSearcher) Count() uint64 { return 0 }

func (s *PerSegmentMatchNoneSearcher) Min() int { return 0 }

func (s *PerSegmentMatchNoneSearcher) Size() int {
	return reflectStaticSizePerSegmentMatchNoneSearcher + size.SizeOfPtr
}

// segCursor implements perSegChild.
func (s *PerSegmentMatchNoneSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	return nil, false
}

// segCursorSeeked implements perSegChild.
func (s *PerSegmentMatchNoneSearcher) segCursorSeeked(seg int, scored bool, seeks uint64) (docCursor, bool) {
	return nil, false
}

// segCost implements perSegChild.
func (s *PerSegmentMatchNoneSearcher) segCost(seg int) uint64 { return 0 }

// numSegments implements perSegChild.
func (s *PerSegmentMatchNoneSearcher) numSegments() int { return 0 }

// ExplainMatch implements search.PerSegmentExplainer: a searcher of no matches has
// none to explain.
func (s *PerSegmentMatchNoneSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	return nil, false, nil
}
