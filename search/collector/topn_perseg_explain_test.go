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

package collector

import (
	"context"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// explainingSearcher is a per segment searcher of docs 0..n-1 in one segment,
// scored by their number, that records when it is asked to explain.
type explainingSearcher struct {
	search.Searcher // never called
	n               int
	next            int
	exhausted       bool
	explained       []uint64
	explainedEarly  bool
}

func (s *explainingSearcher) NextBlock(b *search.PerSegmentScoredBlock) (int, error) {
	if s.next >= s.n {
		s.exhausted = true
		return 0, nil
	}
	k := 0
	for ; k < search.PerSegmentBlockLen && s.next < s.n; k++ {
		b.Docs[k] = uint32(s.next)
		b.Scores[k] = float32(s.next)
		s.next++
	}
	b.Seg, b.Offset, b.MaxScore = 0, 0, float32(s.next-1)
	return k, nil
}

func (s *explainingSearcher) ExplainMatch(seg int, doc uint64) (*search.Explanation, bool, error) {
	if !s.exhausted {
		s.explainedEarly = true
	}
	s.explained = append(s.explained, doc)
	return &search.Explanation{Value: float64(doc), Message: "doc"}, true, nil
}

// reader gives every doc the id its number says
type idReader struct{ index.IndexReader }

func (idReader) ExternalID(id index.IndexInternalID) (string, error) { return string(id), nil }

// The collector asks for explanations once the searcher has been read to its end,
// and only for the hits it returns: not while matches are being found and scored,
// and not for the matches that don't make the page.
func TestPerSegmentCollectorExplainsOnlyTheHitsReturned(t *testing.T) {
	s := &explainingSearcher{n: 1000}
	c := NewPerSegmentTopNCollector(5, 2)
	c.SetExplain(true)
	if err := c.Collect(context.Background(), s, idReader{}); err != nil {
		t.Fatal(err)
	}
	if s.explainedEarly {
		t.Fatal("a hit was explained before the searcher was exhausted")
	}
	res := c.Results()
	if len(res) != 5 || len(s.explained) != 5 {
		t.Fatalf("%d hits, %d explained, want 5 and 5", len(res), len(s.explained))
	}
	for i, hit := range res {
		// the best docs are the highest numbers; skipping the first 2 leaves 997..993
		want := uint64(997 - i)
		if s.explained[i] != want || hit.Expl == nil || hit.Expl.Value != float64(want) {
			t.Fatalf("hit %d: explained doc %d with %v, want doc %d", i, s.explained[i], hit.Expl, want)
		}
	}

	// without SetExplain nothing is explained
	s = &explainingSearcher{n: 1000}
	c = NewPerSegmentTopNCollector(5, 0)
	if err := c.Collect(context.Background(), s, idReader{}); err != nil {
		t.Fatal(err)
	}
	if len(s.explained) != 0 {
		t.Fatalf("%d hits explained without being asked to", len(s.explained))
	}
}

// panickingSearcher is a per segment searcher that goes wrong while it is read
type panickingSearcher struct {
	search.Searcher // never called
	with            func()
}

func (s *panickingSearcher) NextBlock(b *search.PerSegmentScoredBlock) (int, error) {
	s.with()
	return 0, nil
}

// An index out of range in an algorithm fails the search that hit it, with an
// error that says where, and not the process; a panic that isn't one of the
// runtime's is not touched.
func TestPerSegmentCollectorTurnsRuntimePanicsIntoErrors(t *testing.T) {
	var small [4]int
	idx := 5
	s := &panickingSearcher{with: func() { _ = small[idx%8] }}
	err := NewPerSegmentTopNCollector(5, 0).Collect(context.Background(), s, idReader{})
	if err == nil || !strings.Contains(err.Error(), "index out of range") {
		t.Fatalf("got %v, want the index out of range as an error", err)
	}
	if !strings.Contains(err.Error(), "NextBlock") {
		t.Fatalf("the error doesn't say where: %v", err)
	}

	s = &panickingSearcher{with: func() { panic("something else") }}
	func() {
		defer func() {
			if r := recover(); r != "something else" {
				t.Fatalf("recovered %v, want the panic to go through", r)
			}
		}()
		_ = NewPerSegmentTopNCollector(5, 0).Collect(context.Background(), s, idReader{})
		t.Fatal("no panic")
	}()
}
