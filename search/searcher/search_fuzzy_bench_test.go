//  Copyright (c) 2024 Couchbase, Inc.
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
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// buildFuzzyBenchIndex builds a scorch index whose "desc" field has numTerms
// distinct terms spread across numDocs documents (perDoc terms per document),
// so candidate terms have real multi-doc postings lists.
func buildFuzzyBenchIndex(b *testing.B, numTerms, numDocs, perDoc int) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := scorch.NewScorch(scorch.Name,
		map[string]interface{}{"path": b.TempDir()}, analysisQueue)
	if err != nil {
		b.Fatal(err)
	}
	if err = idx.Open(); err != nil {
		b.Fatal(err)
	}

	terms := make([]string, numTerms)
	for i := range terms {
		terms[i] = fmt.Sprintf("term%04d", i)
	}

	batch := index.NewBatch()
	for d := 0; d < numDocs; d++ {
		doc := document.NewDocument(fmt.Sprintf("%d", d))
		buf := make([]byte, 0, perDoc*9)
		for j := 0; j < perDoc; j++ {
			if j > 0 {
				buf = append(buf, ' ')
			}
			buf = append(buf, terms[(d*perDoc+j)%numTerms]...)
		}
		doc.AddField(document.NewTextFieldCustom("desc", nil, buf,
			twoDocIndexDescIndexingOptions, testAnalyzer))
		batch.Update(doc)
	}
	if err = idx.Batch(batch); err != nil {
		b.Fatal(err)
	}
	return idx
}

// BenchmarkFuzzyCandidateCollection isolates candidate-term collection
// (findFuzzyCandidateTerms) over a real scorch index for a fuzziness-2 query.
// This is the step the fuzzy optimizations touch; the full NewFuzzySearcher is
// dominated by building one term searcher per candidate downstream, which these
// changes do not affect and which would dilute the measurement.
func BenchmarkFuzzyCandidateCollection(b *testing.B) {
	idx := buildFuzzyBenchIndex(b, 1000, 3000, 40)
	defer func() { _ = idx.Close() }()

	r, err := idx.Reader()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	ctx := context.Background()

	// sanity: report how many candidates the query matches, once.
	fc, err := findFuzzyCandidateTerms(ctx, r, "term0500", 2, "desc", "")
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("candidates=%d", len(fc.candidates))

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, err := findFuzzyCandidateTerms(ctx, r, "term0500", 2, "desc", "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFuzzySearcherEndToEnd measures the full NewFuzzySearcher path for
// reference (candidate collection + building the boosted disjunction).
func BenchmarkFuzzySearcherEndToEnd(b *testing.B) {
	idx := buildFuzzyBenchIndex(b, 1000, 3000, 40)
	defer func() { _ = idx.Close() }()

	r, err := idx.Reader()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	opts := search.SearcherOptions{Score: "none"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		s, err := NewFuzzySearcher(ctx, r, "term0500", 0, 2, "desc", 1.0, opts)
		if err != nil {
			b.Fatal(err)
		}
		if err = s.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
