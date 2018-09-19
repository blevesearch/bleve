//  Copyright (c) 2015 Couchbase, Inc.
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
	"encoding/binary"
	"fmt"
	"regexp"
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

func TestRegexpSearchUpsideDown(t *testing.T) {
	twoDocIndex := initTwoDocUpsideDown()
	testRegexpSearch(t, twoDocIndex, internalIDMakerUpsideDown, searcherMaker)
	_ = twoDocIndex.Close()
}

func TestRegexpStringSearchUpsideDown(t *testing.T) {
	twoDocIndex := initTwoDocUpsideDown()
	testRegexpSearch(t, twoDocIndex, internalIDMakerUpsideDown, searcherStringMaker)
	_ = twoDocIndex.Close()
}

func TestRegexpSearchScorch(t *testing.T) {
	twoDocIndex := initTwoDocScorch()
	testRegexpSearch(t, twoDocIndex, internalIDMakerScorch, searcherMaker)
	_ = twoDocIndex.Close()
}

func TestRegexpStringSearchScorch(t *testing.T) {
	twoDocIndex := initTwoDocScorch()
	testRegexpSearch(t, twoDocIndex, internalIDMakerScorch, searcherStringMaker)
	_ = twoDocIndex.Close()
}

func internalIDMakerUpsideDown(id int) index.IndexInternalID {
	return index.IndexInternalID(fmt.Sprintf("%d", id))
}

func internalIDMakerScorch(id int) index.IndexInternalID {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(id))
	return index.IndexInternalID(buf)
}

func searcherMaker(t *testing.T, ir index.IndexReader, re, field string) search.Searcher {
	pattern, err := regexp.Compile(re)
	if err != nil {
		t.Fatal(err)
	}

	regexpSearcher, err := NewRegexpSearcher(ir, pattern, field, 1.0,
		search.SearcherOptions{Explain: true})
	if err != nil {
		t.Fatal(err)
	}

	return regexpSearcher
}

func searcherStringMaker(t *testing.T, ir index.IndexReader, re, field string) search.Searcher {
	regexpSearcher, err := NewRegexpStringSearcher(ir, re, field, 1.0,
		search.SearcherOptions{Explain: true})
	if err != nil {
		t.Fatal(err)
	}

	return regexpSearcher
}

func testRegexpSearch(t *testing.T, twoDocIndex index.Index,
	internalIDMaker func(int) index.IndexInternalID,
	searcherMaker func(t *testing.T, ir index.IndexReader, re, field string) search.Searcher) {
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

	regexpSearcher := searcherMaker(t, twoDocIndexReader, "ma.*", "name")
	regexpSearcherCo := searcherMaker(t, twoDocIndexReader, "co.*", "desc")

	tests := []struct {
		searcher  search.Searcher
		expecteds []*search.DocumentMatch
	}{
		{
			searcher: regexpSearcher,
			expecteds: []*search.DocumentMatch{
				{
					IndexInternalID: internalIDMaker(1),
					Score:           1.916290731874155,
				},
			},
		},
		{
			searcher: regexpSearcherCo,
			expecteds: []*search.DocumentMatch{
				{
					IndexInternalID: internalIDMaker(2),
					Score:           0.33875554280828685,
				},
				{
					IndexInternalID: internalIDMaker(3),
					Score:           0.33875554280828685,
				},
			},
		},
	}

	for testIndex, test := range tests {
		defer func() {
			err := test.searcher.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(test.searcher.DocumentMatchPoolSize(), 0),
		}
		next, err := test.searcher.Next(ctx)
		i := 0
		for err == nil && next != nil {
			if i < len(test.expecteds) {
				if !next.IndexInternalID.Equals(test.expecteds[i].IndexInternalID) {
					t.Errorf("test %d, expected result %d to have id %s got %s, next: %#v",
						testIndex, i, test.expecteds[i].IndexInternalID, next.IndexInternalID, next)
				}
				if next.Score != test.expecteds[i].Score {
					t.Errorf("test %d, expected result %d to have score %v got %v,next: %#v",
						testIndex, i, test.expecteds[i].Score, next.Score, next)
					t.Logf("scoring explanation: %s", next.Expl)
				}
			}
			ctx.DocumentMatchPool.Put(next)
			next, err = test.searcher.Next(ctx)
			i++
		}
		if err != nil {
			t.Fatalf("error iterating searcher: %v for test %d", err, testIndex)
		}
		if len(test.expecteds) != i {
			t.Errorf("expected %d results got %d for test %d", len(test.expecteds), i, testIndex)
		}
	}
}
