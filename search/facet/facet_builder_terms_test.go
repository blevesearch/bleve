//  Copyright (c) 2016 Couchbase, Inc.
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

package facet

import (
	"os"
	"regexp"
	"testing"
)

var terms []string

func init() {
	wsRegexp := regexp.MustCompile(`\W+`)
	input, err := os.ReadFile("benchmark_data.txt")
	if err != nil {
		panic(err)
	}
	terms = wsRegexp.Split(string(input), -1)
}

func BenchmarkTermsFacet10(b *testing.B) {
	termsFacetN(b, 10)
}

func BenchmarkTermsFacet100(b *testing.B) {
	termsFacetN(b, 100)
}

func BenchmarkTermsFacet1000(b *testing.B) {
	termsFacetN(b, 1000)
}

func BenchmarkTermsFacet10000(b *testing.B) {
	termsFacetN(b, 10000)
}

// func BenchmarkTermsFacet100000(b *testing.B) {
// 	termsFacetN(b, 100000)
// }

func termsFacetN(b *testing.B, numTerms int) {
	field := "test"
	termsLen := len(terms)
	tfb := NewTermsFacetBuilder(field, 3)
	i := 0
	for len(tfb.termsCount) < numTerms && i <= termsLen {
		j := i % termsLen
		term := terms[j]
		tfb.StartDoc()
		tfb.UpdateVisitor([]byte(term))
		tfb.EndDoc()
		i++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tfb.Result()
	}
}

func TestTermsFacetPrefix(t *testing.T) {
	field := "category"
	tfb := NewTermsFacetBuilder(field, 10)
	tfb.SetPrefixFilter("prod-")

	// Add terms with various prefixes
	terms := []string{
		"prod-server",
		"prod-database",
		"dev-server",
		"dev-database",
		"test-server",
		"prod-cache",
	}

	for _, term := range terms {
		tfb.StartDoc()
		tfb.UpdateVisitor([]byte(term))
		tfb.EndDoc()
	}

	result := tfb.Result()

	// Should only have terms with "prod-" prefix
	if result.Terms.Len() != 3 {
		t.Fatalf("expected 3 matching terms, got %d", result.Terms.Len())
	}

	// Verify the terms are correct
	expectedTerms := map[string]bool{
		"prod-server":   true,
		"prod-database": true,
		"prod-cache":    true,
	}

	for _, facet := range result.Terms.Terms() {
		if !expectedTerms[facet.Term] {
			t.Errorf("unexpected term in results: %s", facet.Term)
		}
		if facet.Count != 1 {
			t.Errorf("expected count 1 for %s, got %d", facet.Term, facet.Count)
		}
	}

	// Total should include all terms (matching + non-matching)
	if result.Total != 6 {
		t.Errorf("expected total 6, got %d", result.Total)
	}

	// Other should be 3 (the non-matching terms)
	if result.Other != 3 {
		t.Errorf("expected other 3, got %d", result.Other)
	}
}

func TestTermsFacetRegex(t *testing.T) {
	field := "product_code"
	// Match pattern: ABC-#### (3 letters, dash, 4 digits) - pattern: ^[A-Z]{3}-\\d{4}$
	tfb := NewTermsFacetBuilder(field, 10)
	regex, err := regexp.Compile("^[A-Z]{3}-\\d{4}$")
	if err != nil {
		t.Fatal(err)
	}
	tfb.SetRegexFilter(regex)

	// Add terms with various formats
	terms := []string{
		"ABC-1234",
		"XYZ-5678",
		"ABC-999",   // too few digits
		"ABCD-1234", // too many letters
		"ABC-ABCD",  // letters instead of digits
		"DEF-0000",
	}

	for _, term := range terms {
		tfb.StartDoc()
		tfb.UpdateVisitor([]byte(term))
		tfb.EndDoc()
	}

	result := tfb.Result()

	// Should only have 3 terms matching the pattern
	if result.Terms.Len() != 3 {
		t.Fatalf("expected 3 matching terms, got %d", result.Terms.Len())
	}

	// Verify the terms are correct
	expectedTerms := map[string]bool{
		"ABC-1234": true,
		"XYZ-5678": true,
		"DEF-0000": true,
	}

	for _, facet := range result.Terms.Terms() {
		if !expectedTerms[facet.Term] {
			t.Errorf("unexpected term in results: %s", facet.Term)
		}
		if facet.Count != 1 {
			t.Errorf("expected count 1 for %s, got %d", facet.Term, facet.Count)
		}
	}

	// Total should include all terms
	if result.Total != 6 {
		t.Errorf("expected total 6, got %d", result.Total)
	}

	// Other should be 3 (the non-matching terms)
	if result.Other != 3 {
		t.Errorf("expected other 3, got %d", result.Other)
	}
}

func TestTermsFacetPrefixAndRegex(t *testing.T) {
	field := "tag"
	// Both prefix "env:" and regex pattern for prod/staging only
	tfb := NewTermsFacetBuilder(field, 10)
	tfb.SetPrefixFilter("env:")
	regex, err := regexp.Compile("^env:(prod|staging)$")
	if err != nil {
		t.Fatal(err)
	}
	tfb.SetRegexFilter(regex)

	// Add various terms
	terms := []string{
		"env:prod",
		"env:staging",
		"env:dev",      // has prefix but doesn't match regex
		"env:test",     // has prefix but doesn't match regex
		"type:server",  // no prefix
		"env:prod",     // duplicate
		"env:staging",  // duplicate
	}

	for _, term := range terms {
		tfb.StartDoc()
		tfb.UpdateVisitor([]byte(term))
		tfb.EndDoc()
	}

	result := tfb.Result()

	// Should only have 2 unique terms (env:prod and env:staging)
	if result.Terms.Len() != 2 {
		t.Fatalf("expected 2 matching terms, got %d", result.Terms.Len())
	}

	// Verify the terms and counts
	termCounts := make(map[string]int)
	for _, facet := range result.Terms.Terms() {
		termCounts[facet.Term] = facet.Count
	}

	if termCounts["env:prod"] != 2 {
		t.Errorf("expected count 2 for env:prod, got %d", termCounts["env:prod"])
	}
	if termCounts["env:staging"] != 2 {
		t.Errorf("expected count 2 for env:staging, got %d", termCounts["env:staging"])
	}

	// Total should be all 7 terms
	if result.Total != 7 {
		t.Errorf("expected total 7, got %d", result.Total)
	}

	// Other should be 3 (env:dev, env:test, type:server)
	if result.Other != 3 {
		t.Errorf("expected other 3, got %d", result.Other)
	}
}

func TestTermsFacetInvalidRegex(t *testing.T) {
	// Invalid regex pattern (unmatched bracket)
	_, err := regexp.Compile("[invalid")
	if err == nil {
		t.Fatal("expected error for invalid regex, got nil")
	}
}

func TestTermsFacetNoFilter(t *testing.T) {
	field := "tag"
	tfb := NewTermsFacetBuilder(field, 2)

	terms := []string{"apple", "banana", "cherry", "apple"}

	for _, term := range terms {
		tfb.StartDoc()
		tfb.UpdateVisitor([]byte(term))
		tfb.EndDoc()
	}

	result := tfb.Result()

	// Should return top 2 by count
	if result.Terms.Len() != 2 {
		t.Fatalf("expected 2 terms, got %d", result.Terms.Len())
	}

	// Apple should be first with count 2
	facets := result.Terms.Terms()
	if facets[0].Term != "apple" || facets[0].Count != 2 {
		t.Errorf("expected apple with count 2, got %s with count %d", facets[0].Term, facets[0].Count)
	}

	// Total should be 4
	if result.Total != 4 {
		t.Errorf("expected total 4, got %d", result.Total)
	}

	// Other should be 1 (cherry was trimmed)
	if result.Other != 1 {
		t.Errorf("expected other 1, got %d", result.Other)
	}
}
