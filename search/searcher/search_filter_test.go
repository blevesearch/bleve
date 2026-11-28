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
	"context"
	"errors"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// filterTestSearcher is a minimal, index-free search.Searcher that yields the
// configured internal IDs in ascending order. It is used to drive FilterSearcher
// without building a real index.
type filterTestSearcher struct {
	ids      []index.IndexInternalID
	nextErr  error
	advErr   error
	closeErr error
	closes   int
	pos      int
}

func (s *filterTestSearcher) Next(*search.SearchContext) (*search.DocumentMatch, error) {
	if s.nextErr != nil {
		return nil, s.nextErr
	}
	if s.pos >= len(s.ids) {
		return nil, nil
	}
	id := s.ids[s.pos]
	s.pos++
	return &search.DocumentMatch{IndexInternalID: id}, nil
}

// Advance returns the first remaining document whose internal ID is >= id,
// mirroring the forward-only contract of real searchers.
func (s *filterTestSearcher) Advance(_ *search.SearchContext,
	id index.IndexInternalID) (*search.DocumentMatch, error) {
	if s.advErr != nil {
		return nil, s.advErr
	}
	for s.pos < len(s.ids) {
		cur := s.ids[s.pos]
		s.pos++
		if cur.Compare(id) >= 0 {
			return &search.DocumentMatch{IndexInternalID: cur}, nil
		}
	}
	return nil, nil
}

func (s *filterTestSearcher) Close() error {
	s.closes++
	return s.closeErr
}

func (s *filterTestSearcher) Weight() float64            { return 1.0 }
func (s *filterTestSearcher) SetQueryNorm(_ float64)     {}
func (s *filterTestSearcher) Count() uint64              { return uint64(len(s.ids)) }
func (s *filterTestSearcher) Min() int                   { return 0 }
func (s *filterTestSearcher) Size() int                  { return 0 }
func (s *filterTestSearcher) DocumentMatchPoolSize() int { return 1 }

func filterTestIDs(ids ...string) []index.IndexInternalID {
	rv := make([]index.IndexInternalID, len(ids))
	for i, id := range ids {
		rv[i] = index.IndexInternalID([]byte(id))
	}
	return rv
}

func newFilterTestContext(s search.Searcher) *search.SearchContext {
	return &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(s.DocumentMatchPoolSize(), 0),
	}
}

func drainFilterSearcher(t *testing.T, s search.Searcher) []string {
	t.Helper()
	ctx := newFilterTestContext(s)
	var got []string
	for {
		doc, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error from Next: %v", err)
		}
		if doc == nil {
			break
		}
		got = append(got, string(doc.IndexInternalID))
	}
	return got
}

func filterTestEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFilterSearcherKeepsOnlyMatchingDocuments(t *testing.T) {
	tests := []struct {
		name   string
		child  []string
		filter []string
		want   []string
	}{
		{
			name:   "interleaved",
			child:  []string{"1", "2", "3", "4"},
			filter: []string{"2", "4"},
			want:   []string{"2", "4"},
		},
		{
			name:   "filter is a superset",
			child:  []string{"2", "4"},
			filter: []string{"1", "2", "3", "4"},
			want:   []string{"2", "4"},
		},
		{
			name:   "disjoint sets",
			child:  []string{"1", "2"},
			filter: []string{"8", "9"},
			want:   nil,
		},
		{
			name:   "empty filter",
			child:  []string{"1", "2"},
			filter: nil,
			want:   nil,
		},
		{
			name:   "filter ahead of child",
			child:  []string{"3"},
			filter: []string{"5"},
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			child := &filterTestSearcher{ids: filterTestIDs(tc.child...)}
			filter := &filterTestSearcher{ids: filterTestIDs(tc.filter...)}
			s := NewFilterSearcher(context.TODO(), child, filter)

			got := drainFilterSearcher(t, s)
			if !filterTestEqualStrings(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilterSearcherAdvance(t *testing.T) {
	child := &filterTestSearcher{ids: filterTestIDs("1", "2", "3", "4")}
	filter := &filterTestSearcher{ids: filterTestIDs("2", "4")}
	s := NewFilterSearcher(context.TODO(), child, filter)
	ctx := newFilterTestContext(s)

	doc, err := s.Advance(ctx, index.IndexInternalID([]byte("3")))
	if err != nil {
		t.Fatalf("unexpected error from Advance: %v", err)
	}
	if doc == nil {
		t.Fatal("expected Advance to return a document, got nil")
	}
	if got := string(doc.IndexInternalID); got != "4" {
		t.Fatalf("expected Advance to land on 4, got %q", got)
	}
}

func TestFilterSearcherPropagatesFilterNextError(t *testing.T) {
	injected := errors.New("filter next failed")
	child := &filterTestSearcher{ids: filterTestIDs("1")}
	filter := &filterTestSearcher{nextErr: injected}
	s := NewFilterSearcher(context.TODO(), child, filter)

	_, err := s.Next(newFilterTestContext(s))
	if !errors.Is(err, injected) {
		t.Fatalf("expected Next to return %v, got %v", injected, err)
	}
}

func TestFilterSearcherPropagatesFilterAdvanceError(t *testing.T) {
	injected := errors.New("filter advance failed")

	newSearcher := func() *FilterSearcher {
		child := &filterTestSearcher{ids: filterTestIDs("1")}
		// filter's first document sorts before the child's, so matching a child
		// document requires advancing the filter searcher.
		filter := &filterTestSearcher{ids: filterTestIDs("0"), advErr: injected}
		return NewFilterSearcher(context.TODO(), child, filter)
	}

	s := newSearcher()
	if _, err := s.Next(newFilterTestContext(s)); !errors.Is(err, injected) {
		t.Fatalf("expected Next to return %v, got %v", injected, err)
	}

	s = newSearcher()
	if _, err := s.Advance(newFilterTestContext(s), index.IndexInternalID([]byte("1"))); !errors.Is(err, injected) {
		t.Fatalf("expected Advance to return %v, got %v", injected, err)
	}
}

func TestFilterSearcherCloseClosesBothSearchers(t *testing.T) {
	filterErr := errors.New("filter close failed")
	child := &filterTestSearcher{}
	filter := &filterTestSearcher{closeErr: filterErr}
	s := NewFilterSearcher(context.TODO(), child, filter)

	err := s.Close()
	if !errors.Is(err, filterErr) {
		t.Fatalf("expected Close to return %v, got %v", filterErr, err)
	}
	if child.closes != 1 {
		t.Errorf("expected child to be closed once, got %d", child.closes)
	}
	if filter.closes != 1 {
		t.Errorf("expected filter searcher to be closed once, got %d", filter.closes)
	}
}

func TestFilterSearcherClosePrefersChildError(t *testing.T) {
	childErr := errors.New("child close failed")
	filterErr := errors.New("filter close failed")
	child := &filterTestSearcher{closeErr: childErr}
	filter := &filterTestSearcher{closeErr: filterErr}
	s := NewFilterSearcher(context.TODO(), child, filter)

	err := s.Close()
	if !errors.Is(err, childErr) {
		t.Fatalf("expected Close to return the child error %v, got %v", childErr, err)
	}
	if child.closes != 1 || filter.closes != 1 {
		t.Errorf("expected both searchers to be closed once, got child=%d filter=%d",
			child.closes, filter.closes)
	}
}

func TestFilterSearcherDocumentMatchPoolSize(t *testing.T) {
	child := &filterTestSearcher{}
	filter := &filterTestSearcher{}
	s := NewFilterSearcher(context.TODO(), child, filter)

	want := child.DocumentMatchPoolSize() + filter.DocumentMatchPoolSize()
	if got := s.DocumentMatchPoolSize(); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}
