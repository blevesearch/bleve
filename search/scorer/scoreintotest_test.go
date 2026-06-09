// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorer

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// TestScoreIntoMatchesScore verifies that ScoreInto (§9 lazy BM25 path)
// produces the same Score as Score() for the same TermFieldDoc, and that
// FieldTermLocations are populated identically.
func TestScoreIntoMatchesScore(t *testing.T) {
	const docTotal uint64 = 100
	const docTerm uint64 = 10
	scorer := NewTermQueryScorer(
		[]byte("beer"), "desc", 1.0, docTotal, docTerm,
		50.0, // avgDocLength
		search.SearcherOptions{Explain: false},
	)
	scorer.SetQueryNorm(1.0) // required to initialize idfQueryWeight

	tfd := &index.TermFieldDoc{
		ID:   index.IndexInternalID("doc1"),
		Freq: 3,
		Norm: float64(float32(1.0 / math.Sqrt(5))), // fieldLen=5
		Vectors: []*index.TermFieldVector{
			{Field: "desc", Pos: 1, Start: 0, End: 4},
			{Field: "desc", Pos: 2, Start: 5, End: 9},
		},
	}

	// Score() path.
	ctx := &search.SearchContext{DocumentMatchPool: search.NewDocumentMatchPool(10, 0)}
	scoredMatch := scorer.Score(ctx, tfd)

	// ScoreInto() path.
	rv := &search.DocumentMatch{}
	scorer.ScoreInto(tfd, rv)

	// Scores must match within float64 rounding.
	if math.Abs(rv.Score-scoredMatch.Score) > 1e-10 {
		t.Errorf("ScoreInto Score=%f, Score() gave %f", rv.Score, scoredMatch.Score)
	}

	// FieldTermLocations must be populated with the same entries.
	if len(rv.FieldTermLocations) != len(tfd.Vectors) {
		t.Fatalf("ScoreInto: FieldTermLocations len=%d, want %d",
			len(rv.FieldTermLocations), len(tfd.Vectors))
	}
	for i, v := range tfd.Vectors {
		ftl := rv.FieldTermLocations[i]
		if ftl.Field != v.Field {
			t.Errorf("[%d] Field: got %q, want %q", i, ftl.Field, v.Field)
		}
		if ftl.Location.Pos != v.Pos {
			t.Errorf("[%d] Pos: got %d, want %d", i, ftl.Location.Pos, v.Pos)
		}
		if ftl.Location.Start != v.Start {
			t.Errorf("[%d] Start: got %d, want %d", i, ftl.Location.Start, v.Start)
		}
		if ftl.Location.End != v.End {
			t.Errorf("[%d] End: got %d, want %d", i, ftl.Location.End, v.End)
		}
	}
}

// TestScoreIntoNoVectors verifies ScoreInto does not set FieldTermLocations
// when the TermFieldDoc has no vectors.
func TestScoreIntoNoVectors(t *testing.T) {
	scorer := NewTermQueryScorer(
		[]byte("foo"), "f", 1.0, 100, 10, 20.0,
		search.SearcherOptions{},
	)
	scorer.SetQueryNorm(1.0)

	tfd := &index.TermFieldDoc{
		ID:   index.IndexInternalID("x"),
		Freq: 1,
		Norm: 1.0,
	}
	rv := &search.DocumentMatch{}
	scorer.ScoreInto(tfd, rv)

	if len(rv.FieldTermLocations) != 0 {
		t.Errorf("expected no FieldTermLocations, got %d", len(rv.FieldTermLocations))
	}
	if rv.Score <= 0 {
		t.Errorf("expected positive score, got %f", rv.Score)
	}
}

// TestScoreIntoTablePathMatchesFormula verifies the §25 impact-table fast path
// inside ScoreInto gives the same score as the formula path.  The table path
// is active when impactTable != nil AND NormByte != 0 AND Freq < MaxSqrtCache.
func TestScoreIntoTablePathMatchesFormula(t *testing.T) {
	const avgDocLen = 50.0
	scorer := NewTermQueryScorer(
		[]byte("hello"), "body", 1.0, 1000, 100, avgDocLen,
		search.SearcherOptions{Explain: false},
	)
	scorer.SetQueryNorm(1.0) // required to initialize idfQueryWeight

	// normByte=0x5c is a common value (corresponds to fieldLen≈3)
	const normByte = uint8(0x5c)
	const freq = uint64(2)

	// Table path (NormByte != 0, Freq < MaxSqrtCache, impactTable != nil).
	tfdTable := &index.TermFieldDoc{
		ID:       index.IndexInternalID("a"),
		Freq:     freq,
		NormByte: normByte,
	}
	rvTable := &search.DocumentMatch{}
	scorer.ScoreInto(tfdTable, rvTable)

	// Formula path: set NormByte=0 to force the non-table branch.
	// Compute expected norm from the SmallFloat byte to match what the table uses.
	fieldLen := bm25SmallFloatFieldLen(normByte)
	norm := float64(float32(1.0 / math.Sqrt(fieldLen)))
	tfdFormula := &index.TermFieldDoc{
		ID:       index.IndexInternalID("a"),
		Freq:     freq,
		Norm:     norm,
		NormByte: 0, // disable table path
	}
	rvFormula := &search.DocumentMatch{}
	scorer.ScoreInto(tfdFormula, rvFormula)

	diff := math.Abs(rvTable.Score - rvFormula.Score)
	// float32→float64 conversion from table vs full float64 formula: allow 0.01% relative error.
	if rvFormula.Score > 0 {
		relErr := diff / rvFormula.Score
		if relErr > 1e-4 {
			t.Errorf("table path score %f vs formula score %f (relErr %.2e)",
				rvTable.Score, rvFormula.Score, relErr)
		}
	}
}
