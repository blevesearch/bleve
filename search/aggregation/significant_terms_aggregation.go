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

package aggregation

import (
	"context"
	"math"
	"reflect"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeSignificantTermsAggregation int

func init() {
	var sta SignificantTermsAggregation
	reflectStaticSizeSignificantTermsAggregation = int(reflect.TypeOf(sta).Size())
}

// SignificanceAlgorithm defines the scoring algorithm for significant terms
type SignificanceAlgorithm string

const (
	// JLH (default) - measures "uncommonly common" terms
	SignificanceAlgorithmJLH SignificanceAlgorithm = "jlh"
	// MutualInformation - information gain from term presence
	SignificanceAlgorithmMutualInformation SignificanceAlgorithm = "mutual_information"
	// ChiSquared - chi-squared statistical test
	SignificanceAlgorithmChiSquared SignificanceAlgorithm = "chi_squared"
	// Percentage - simple ratio comparison
	SignificanceAlgorithmPercentage SignificanceAlgorithm = "percentage"
)

// SignificantTermsAggregation finds "uncommonly common" terms in query results
// compared to their frequency in the overall index (background set)
type SignificantTermsAggregation struct {
	field       string
	size        int
	minDocCount int64
	algorithm   SignificanceAlgorithm

	// Phase 1: Collect foreground data (from query results)
	foregroundTerms    map[string]int64 // term -> doc count in results
	foregroundDocCount int64             // total docs in results
	currentTerm        string
	sawValue           bool

	// Phase 2: Background statistics (from pre-search or index reader)
	backgroundStats *search.SignificantTermsStats
	indexReader     index.IndexReader // For fallback when no pre-search data
}

// NewSignificantTermsAggregation creates a new significant terms aggregation
func NewSignificantTermsAggregation(field string, size int, minDocCount int64, algorithm SignificanceAlgorithm) *SignificantTermsAggregation {
	if size <= 0 {
		size = 10 // default
	}
	if minDocCount < 0 {
		minDocCount = 0
	}
	if algorithm == "" {
		algorithm = SignificanceAlgorithmJLH // default
	}

	return &SignificantTermsAggregation{
		field:           field,
		size:            size,
		minDocCount:     minDocCount,
		algorithm:       algorithm,
		foregroundTerms: make(map[string]int64),
	}
}

func (sta *SignificantTermsAggregation) Size() int {
	sizeInBytes := reflectStaticSizeSignificantTermsAggregation + size.SizeOfPtr +
		len(sta.field)

	for term := range sta.foregroundTerms {
		sizeInBytes += size.SizeOfString + len(term) + 8 // int64 = 8 bytes
	}
	return sizeInBytes
}

func (sta *SignificantTermsAggregation) Field() string {
	return sta.field
}

func (sta *SignificantTermsAggregation) Type() string {
	return "significant_terms"
}

func (sta *SignificantTermsAggregation) StartDoc() {
	sta.sawValue = false
	sta.currentTerm = ""
	sta.foregroundDocCount++
}

func (sta *SignificantTermsAggregation) UpdateVisitor(field string, term []byte) {
	if field != sta.field {
		return
	}

	if !sta.sawValue {
		sta.sawValue = true
		termStr := string(term)
		sta.currentTerm = termStr
		sta.foregroundTerms[termStr]++
	}
}

func (sta *SignificantTermsAggregation) EndDoc() {
	// Nothing to do - we only count first occurrence per document
}

// SetBackgroundStats sets the pre-computed background statistics
// This is called when pre-search data is available
func (sta *SignificantTermsAggregation) SetBackgroundStats(stats *search.SignificantTermsStats) {
	sta.backgroundStats = stats
}

// SetIndexReader sets the index reader for fallback background term lookups
// This is used when pre-search is not available
func (sta *SignificantTermsAggregation) SetIndexReader(reader index.IndexReader) {
	sta.indexReader = reader
}

func (sta *SignificantTermsAggregation) Result() *search.AggregationResult {
	// Get background statistics
	var totalDocs int64
	termDocFreqs := make(map[string]int64)

	if sta.backgroundStats != nil {
		// Use pre-search data (multi-shard scenario)
		totalDocs = sta.backgroundStats.TotalDocs
		termDocFreqs = sta.backgroundStats.TermDocFreqs
	} else if sta.indexReader != nil {
		// Fallback: lookup from index reader (single index scenario)
		count, _ := sta.indexReader.DocCount()
		totalDocs = int64(count)

		// Look up background frequency for each foreground term
		ctx := context.Background()
		for term := range sta.foregroundTerms {
			tfr, err := sta.indexReader.TermFieldReader(ctx, []byte(term), sta.field, false, false, false)
			if err == nil && tfr != nil {
				termDocFreqs[term] = int64(tfr.Count())
				tfr.Close()
			}
		}
	} else {
		// No background data available - return empty result
		return &search.AggregationResult{
			Field:   sta.field,
			Type:    "significant_terms",
			Buckets: []*search.Bucket{},
		}
	}

	if totalDocs == 0 {
		totalDocs = sta.foregroundDocCount // prevent division by zero
	}

	// Score each term
	type scoredTerm struct {
		term    string
		score   float64
		fgCount int64
		bgCount int64
	}

	scored := make([]scoredTerm, 0, len(sta.foregroundTerms))

	for term, fgCount := range sta.foregroundTerms {
		// Skip terms below minimum doc count threshold
		if fgCount < sta.minDocCount {
			continue
		}

		bgCount := termDocFreqs[term]
		if bgCount == 0 {
			bgCount = fgCount // Handle case where term wasn't in background (shouldn't happen normally)
		}

		// Calculate significance score
		score := sta.calculateScore(fgCount, sta.foregroundDocCount, bgCount, totalDocs)

		scored = append(scored, scoredTerm{
			term:    term,
			score:   score,
			fgCount: fgCount,
			bgCount: bgCount,
		})
	}

	// Sort by score (descending)
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			// Tie-break by foreground count
			return scored[i].fgCount > scored[j].fgCount
		}
		return scored[i].score > scored[j].score
	})

	// Take top N
	if len(scored) > sta.size {
		scored = scored[:sta.size]
	}

	// Build result buckets
	buckets := make([]*search.Bucket, len(scored))
	for i, st := range scored {
		buckets[i] = &search.Bucket{
			Key:   st.term,
			Count: st.fgCount,
			Metadata: map[string]interface{}{
				"score":    st.score,
				"bg_count": st.bgCount,
			},
		}
	}

	return &search.AggregationResult{
		Field:   sta.field,
		Type:    "significant_terms",
		Buckets: buckets,
		Metadata: map[string]interface{}{
			"algorithm":       string(sta.algorithm),
			"fg_doc_count":    sta.foregroundDocCount,
			"bg_doc_count":    totalDocs,
			"unique_terms":    len(sta.foregroundTerms),
			"significant_terms": len(buckets),
		},
	}
}

func (sta *SignificantTermsAggregation) Clone() search.AggregationBuilder {
	return NewSignificantTermsAggregation(sta.field, sta.size, sta.minDocCount, sta.algorithm)
}

// calculateScore computes the significance score based on the configured algorithm
func (sta *SignificantTermsAggregation) calculateScore(fgCount, fgTotal, bgCount, bgTotal int64) float64 {
	switch sta.algorithm {
	case SignificanceAlgorithmJLH:
		return calculateJLH(fgCount, fgTotal, bgCount, bgTotal)
	case SignificanceAlgorithmMutualInformation:
		return calculateMutualInformation(fgCount, fgTotal, bgCount, bgTotal)
	case SignificanceAlgorithmChiSquared:
		return calculateChiSquared(fgCount, fgTotal, bgCount, bgTotal)
	case SignificanceAlgorithmPercentage:
		return calculatePercentage(fgCount, fgTotal, bgCount, bgTotal)
	default:
		return calculateJLH(fgCount, fgTotal, bgCount, bgTotal)
	}
}

// calculateJLH computes the JLH (Johnson-Lindenstrauss-Hashing) score
// Measures how "uncommonly common" a term is (high in foreground, low in background)
func calculateJLH(fgCount, fgTotal, bgCount, bgTotal int64) float64 {
	if fgTotal == 0 || bgTotal == 0 || bgCount == 0 {
		return 0
	}

	fgRate := float64(fgCount) / float64(fgTotal)
	bgRate := float64(bgCount) / float64(bgTotal)

	if bgRate == 0 || fgRate <= bgRate {
		return 0
	}

	// JLH = fgRate * log2(fgRate / bgRate)
	score := fgRate * math.Log2(fgRate/bgRate)
	return score
}

// calculateMutualInformation computes mutual information between term and result set
// Measures information gain from knowing whether a document contains the term
func calculateMutualInformation(fgCount, fgTotal, bgCount, bgTotal int64) float64 {
	N := float64(bgTotal)
	if N == 0 {
		return 0
	}

	// Ensure bgCount is at least fgCount (can happen with stale stats)
	if bgCount < fgCount {
		bgCount = fgCount
	}

	// Contingency table:
	// N11 = term present, in results
	// N10 = term present, not in results
	// N01 = term absent, in results
	// N00 = term absent, not in results
	N11 := float64(fgCount)
	N10 := float64(bgCount - fgCount)
	N01 := float64(fgTotal - fgCount)
	N00 := N - N11 - N10 - N01

	if N11 <= 0 || N10 < 0 || N01 < 0 || N00 < 0 {
		return 0
	}

	// Handle edge case where all cells must be positive for MI calculation
	if N10 == 0 || N01 == 0 {
		// When N10 or N01 is 0, use a simple score based on enrichment
		return float64(fgCount) / float64(fgTotal)
	}

	// Mutual information formula
	score := (N11 / N) * math.Log2((N*N11)/((N11+N10)*(N11+N01)))
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0
	}
	return score
}

// calculateChiSquared computes chi-squared statistical test
// Measures how much the observed frequency deviates from expected frequency
func calculateChiSquared(fgCount, fgTotal, bgCount, bgTotal int64) float64 {
	if fgTotal == 0 || bgTotal == 0 {
		return 0
	}

	N := float64(bgTotal)
	observed := float64(fgCount)
	expected := (float64(fgTotal) * float64(bgCount)) / N

	if expected == 0 {
		return 0
	}

	// Chi-squared = (observed - expected)^2 / expected
	chiSquared := math.Pow(observed-expected, 2) / expected
	if math.IsNaN(chiSquared) || math.IsInf(chiSquared, 0) {
		return 0
	}
	return chiSquared
}

// calculatePercentage computes simple percentage score
// Ratio of foreground rate to background rate
func calculatePercentage(fgCount, fgTotal, bgCount, bgTotal int64) float64 {
	if fgTotal == 0 || bgTotal == 0 || bgCount == 0 {
		return 0
	}

	fgRate := float64(fgCount) / float64(fgTotal)
	bgRate := float64(bgCount) / float64(bgTotal)

	if bgRate == 0 {
		return 0
	}

	// Percentage score = (fgRate / bgRate) - 1
	score := (fgRate / bgRate) - 1.0
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return 0
	}
	return score
}

// CollectBackgroundTermStats collects background term statistics for significant_terms
// If terms is nil/empty, collects stats for ALL terms in the field (used during pre-search)
// If terms is provided, collects stats only for those specific terms
func CollectBackgroundTermStats(ctx context.Context, indexReader index.IndexReader, field string, terms []string) (*search.SignificantTermsStats, error) {
	count, err := indexReader.DocCount()
	if err != nil {
		return nil, err
	}

	termDocFreqs := make(map[string]int64)

	// If no specific terms provided, collect ALL terms from field dictionary (pre-search mode)
	if len(terms) == 0 {
		dict, err := indexReader.FieldDict(field)
		if err != nil {
			return nil, err
		}
		defer dict.Close()

		de, err := dict.Next()
		for err == nil && de != nil {
			termDocFreqs[de.Term] = int64(de.Count)
			de, err = dict.Next()
		}
	} else {
		// Collect stats only for specific terms
		for _, term := range terms {
			tfr, err := indexReader.TermFieldReader(ctx, []byte(term), field, false, false, false)
			if err == nil && tfr != nil {
				termDocFreqs[term] = int64(tfr.Count())
				tfr.Close()
			}
		}
	}

	return &search.SignificantTermsStats{
		Field:        field,
		TotalDocs:    int64(count),
		TermDocFreqs: termDocFreqs,
	}, nil
}
