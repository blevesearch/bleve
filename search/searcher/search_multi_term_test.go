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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// ctxWithTermSearcherLimit caps the number of leaf term searchers, giving us a
// deterministic way to make term searcher construction fail on purpose.
func ctxWithTermSearcherLimit(limit int) context.Context {
	ctx := context.WithValue(context.Background(), search.MaxTermSearchersKey, limit)
	return search.ContextWithTermSearchersCounter(ctx)
}

// A term searcher that fails to build must come back as a nil search.Searcher
// interface, never as an interface holding a nil *TermSearcher. Callers guard
// their cleanup with `if searcher != nil`, and a typed nil slips past that guard
// and panics on a nil receiver in (*TermSearcher).Close.
func TestTermSearcherErrorReturnsNilSearcher(t *testing.T) {
	twoDocIndexReader, err := twoDocIndex.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := twoDocIndexReader.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Room for exactly one leaf term searcher, so the second one fails inside
	// newTermSearcherFromReader.
	ctx := ctxWithTermSearcherLimit(1)

	first, err := NewTermSearcher(ctx, twoDocIndexReader, "beer", "desc", 1.0,
		search.SearcherOptions{})
	if err != nil {
		t.Fatalf("first term searcher should be within the limit, got: %v", err)
	}
	defer func() {
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	second, err := NewTermSearcher(ctx, twoDocIndexReader, "angst", "desc", 1.0,
		search.SearcherOptions{})
	if err == nil {
		_ = second.Close()
		t.Fatal("second term searcher should have exceeded the limit of 1")
	}
	if second != nil {
		t.Fatalf("expected a nil search.Searcher alongside the error, got an "+
			"interface holding %T (typed nil: %v)", second, second == search.Searcher(nil))
	}
}

// Regression test for the panic in makeBatchSearchers' cleanup: when one term in
// the batch fails to build, closing the already-built searchers must not trip
// over the failed entry.
func TestMultiTermSearcherCleanupAfterError(t *testing.T) {
	twoDocIndexReader, err := twoDocIndex.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := twoDocIndexReader.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Two of the three terms build; the third trips the limit, sending
	// makeBatchSearchers down its qsearchersClose path.
	ctx := ctxWithTermSearcherLimit(2)
	terms := []string{"beer", "angst", "couch"}

	searcher, err := NewMultiTermSearcher(ctx, twoDocIndexReader, terms, "desc",
		1.0, search.SearcherOptions{}, true)
	if err == nil {
		_ = searcher.Close()
		t.Fatalf("%d terms should have exceeded the limit of 2", len(terms))
	}
	if searcher != nil {
		t.Fatalf("expected a nil search.Searcher alongside the error, got %T", searcher)
	}
}
