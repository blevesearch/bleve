//  Copyright (c) 2017 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/index/scorch"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/search"
)

func TestTermRangeSearch(t *testing.T) {

	twoDocIndexReader, err := twoDocIndex.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := twoDocIndexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	tests := []struct {
		min          []byte
		max          []byte
		inclusiveMin bool
		inclusiveMax bool
		field        string
		want         []string
	}{
		{
			min:          []byte("marty"),
			max:          []byte("marty"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         []string{"1"},
		},
		{
			min:          []byte("marty"),
			max:          []byte("ravi"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         []string{"1", "4"},
		},
		// inclusive max false should exclude ravi
		{
			min:          []byte("marty"),
			max:          []byte("ravi"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: false,
			want:         []string{"1"},
		},
		// inclusive max false should remove last/only item
		{
			min:          []byte("martz"),
			max:          []byte("ravi"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: false,
			want:         nil,
		},
		// inclusive min false should remove marty
		{
			min:          []byte("marty"),
			max:          []byte("ravi"),
			field:        "name",
			inclusiveMin: false,
			inclusiveMax: true,
			want:         []string{"4"},
		},
		// inclusive min false should remove first/only item
		{
			min:          []byte("marty"),
			max:          []byte("rav"),
			field:        "name",
			inclusiveMin: false,
			inclusiveMax: true,
			want:         nil,
		},
		// max nil sees everything after marty
		{
			min:          []byte("marty"),
			max:          nil,
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         []string{"1", "2", "4"},
		},
		// min nil sees everything before ravi
		{
			min:          nil,
			max:          []byte("ravi"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         []string{"1", "3", "4", "5"},
		},
		// min and max nil sees everything
		{
			min:          nil,
			max:          nil,
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         []string{"1", "2", "3", "4", "5"},
		},
		// min and max nil sees everything, even with inclusiveMin false
		{
			min:          nil,
			max:          nil,
			field:        "name",
			inclusiveMin: false,
			inclusiveMax: true,
			want:         []string{"1", "2", "3", "4", "5"},
		},
		// min and max nil sees everything, even with inclusiveMax false
		{
			min:          nil,
			max:          nil,
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: false,
			want:         []string{"1", "2", "3", "4", "5"},
		},
		// min and max nil sees everything, even with both false
		{
			min:          nil,
			max:          nil,
			field:        "name",
			inclusiveMin: false,
			inclusiveMax: false,
			want:         []string{"1", "2", "3", "4", "5"},
		},
		// min and max non-nil, but match 0 terms
		{
			min:          []byte("martz"),
			max:          []byte("rav"),
			field:        "name",
			inclusiveMin: true,
			inclusiveMax: true,
			want:         nil,
		},
		// min and max same (and term exists), both exlusive
		{
			min:          []byte("marty"),
			max:          []byte("marty"),
			field:        "name",
			inclusiveMin: false,
			inclusiveMax: false,
			want:         nil,
		},
	}

	for _, test := range tests {

		searcher, err := NewTermRangeSearcher(twoDocIndexReader, test.min, test.max,
			&test.inclusiveMin, &test.inclusiveMax, test.field, 1.0, search.SearcherOptions{Explain: true})
		if err != nil {
			t.Fatal(err)
		}

		var got []string
		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(
				searcher.DocumentMatchPoolSize(), 0),
		}
		next, err := searcher.Next(ctx)
		i := 0
		for err == nil && next != nil {
			got = append(got, string(next.IndexInternalID))
			ctx.DocumentMatchPool.Put(next)
			next, err = searcher.Next(ctx)
			i++
		}
		if err != nil {
			t.Fatalf("error iterating searcher: %v", err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("expected: %v, got %v for test %#v", test.want, got, test)
		}

	}

}

func TestTermRangeSearchTooManyTerms(t *testing.T) {
	dir, _ := ioutil.TempDir("", "scorchTwoDoc")
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	scorchIndex := initTwoDocScorch(dir)

	// use lower limit for this test
	origLimit := DisjunctionMaxClauseCount
	DisjunctionMaxClauseCount = 2
	defer func() {
		DisjunctionMaxClauseCount = origLimit
	}()

	scorchReader, err := scorchIndex.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := scorchReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var want = []string{"1", "3", "4", "5"}
	var truth = true
	searcher, err := NewTermRangeSearcher(scorchReader, []byte("bobert"), []byte("ravi"),
		&truth, &truth, "name", 1.0, search.SearcherOptions{Score: "none", IncludeTermVectors: false})
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(
			searcher.DocumentMatchPoolSize(), 0),
	}
	next, err := searcher.Next(ctx)
	i := 0
	for err == nil && next != nil {
		extId, err := scorchReader.ExternalID(next.IndexInternalID)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, extId)
		ctx.DocumentMatchPool.Put(next)
		next, err = searcher.Next(ctx)
		i++
	}
	if err != nil {
		t.Fatalf("error iterating searcher: %v", err)
	}
	err = searcher.Close()
	if err != nil {
		t.Fatal(err)
	}

	// check that the expected number of term searchers were started
	// 6 = 4 original terms, 1 optimized after first round, then final searcher
	// from the last round
	statsMap := scorchIndex.(*scorch.Scorch).StatsMap()
	if statsMap["term_searchers_started"].(uint64) != 6 {
		t.Errorf("expected 6 term searchers started, got %d", statsMap["term_searchers_started"])
	}
	// check that all started searchers were closed
	if statsMap["term_searchers_started"] != statsMap["term_searchers_finished"] {
		t.Errorf("expected all term searchers closed, %d started %d closed",
			statsMap["term_searchers_started"], statsMap["term_searchers_finished"])
	}

	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected: %#v, got %#v", want, got)
	}
}
