//  Copyright (c) 2019 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/search"
	"testing"
)

func TestCheckArrayDepth(t *testing.T) {
	d := &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "tshirts.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1}}},
			{Field: "tshirts.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{1}},
			}}}

	ctx := depthFilterContext{arrayDepth: 1}
	if !ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be true at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "tshirts.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1}}},
			{Field: "size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: nil},
			}}}

	ctx = depthFilterContext{arrayDepth: 1}
	if ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be false at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "tshirts.LP.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.LP.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0, 0}}},
			{Field: "tshirts.Arrow.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 2}}},
			{Field: "tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{1, 0}}},
		}}

	ctx = depthFilterContext{arrayDepth: 2}
	if !ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be true at depth: %d, but got: %v", ctx.arrayDepth, true)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "tshirts.LP.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.LP.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0, 0}}},
			{Field: "tshirts.Arrow.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 2}}},
			{Field: "tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{1, 0}}},
		}}

	ctx = depthFilterContext{arrayDepth: 1}
	if ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be false at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "tshirts.LP.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.LP.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0, 0}}},
			{Field: "tshirts.Arrow.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 0}}},
			{Field: "tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{1, 2}}},
			{Field: "tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{0, 2}}},
			{Field: "tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{1, 2}}},
		}}

	ctx = depthFilterContext{arrayDepth: 2}
	if !ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be true at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "country.india.tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0, 0, 3, 2}}},
			{Field: "country.india.tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{0, 0, 2, 2}}},
			{Field: "country.india.tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{0, 0, 3, 2}}},
		}}

	ctx = depthFilterContext{arrayDepth: 4}
	if !ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be true at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

	d = &search.DocumentMatch{
		FieldTermLocations: []search.FieldTermLocation{
			{Field: "country.india.tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{0, 0, 9, 2}}},
			{Field: "country.india.tshirts.Arrow.metrics.color", Term: "blue",
				Location: search.Location{Pos: 1, Start: 0, End: 4, ArrayPositions: []uint64{4, 0, 9, 2}}},
			{Field: "country.india.tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{0, 0, 3, 2}}},
			{Field: "country.india.tshirts.LP.metrics.size", Term: "small",
				Location: search.Location{Pos: 1, Start: 0, End: 5, ArrayPositions: []uint64{4, 0, 9, 2}}},
		}}

	ctx = depthFilterContext{arrayDepth: 4}
	if !ctx.checkArrayDepth(d) {
		t.Errorf("expected  checkArrayDepth to be true at depth: %d, but got: %v", ctx.arrayDepth, false)
	}

}
