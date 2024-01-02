//  Copyright (c) 2014 Couchbase, Inc.
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

package search

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTermFacetResultsMerge(t *testing.T) {
	type testCase struct {
		// Input
		frs1   FacetResults   // first facet results
		frs2   FacetResults   // second facet results (to be merged into first)
		fixups map[string]int // {facetName:size} (to be applied after merge)

		// Expected output
		expFrs FacetResults // facet results after merge and fixup
	}

	tests := []*testCase{
		func() *testCase {
			rv := &testCase{}

			rv.frs1 = FacetResults{
				"types": &FacetResult{
					Field:   "type",
					Total:   100,
					Missing: 25,
					Other:   25,
					Terms: func() *TermFacets {
						tfs := &TermFacets{}
						tfs.Add(
							&TermFacet{
								Term:  "blog",
								Count: 25,
							},
							&TermFacet{
								Term:  "comment",
								Count: 24,
							},
							&TermFacet{
								Term:  "feedback",
								Count: 1,
							},
						)
						return tfs
					}(),
				},
				"categories": &FacetResult{
					Field:   "category",
					Total:   97,
					Missing: 22,
					Other:   15,
					Terms: func() *TermFacets {
						tfs := &TermFacets{}
						tfs.Add(
							&TermFacet{
								Term:  "clothing",
								Count: 35,
							},
							&TermFacet{
								Term:  "electronics",
								Count: 25,
							},
						)
						return tfs
					}(),
				},
			}
			rv.frs2 = FacetResults{
				"types": &FacetResult{
					Field:   "type",
					Total:   100,
					Missing: 25,
					Other:   25,
					Terms: func() *TermFacets {
						tfs := &TermFacets{}
						tfs.Add(
							&TermFacet{
								Term:  "blog",
								Count: 25,
							},
							&TermFacet{
								Term:  "comment",
								Count: 22,
							},
							&TermFacet{
								Term:  "flag",
								Count: 3,
							},
						)
						return tfs
					}(),
				},
			}
			rv.fixups = map[string]int{
				"types": 3, // we want top 3 terms based on count
			}

			rv.expFrs = FacetResults{
				"types": &FacetResult{
					Field:   "type",
					Total:   200,
					Missing: 50,
					Other:   51,
					Terms: &TermFacets{
						termFacets: []*TermFacet{
							{
								Term:  "blog",
								Count: 50,
							},
							{
								Term:  "comment",
								Count: 46,
							},
							{
								Term:  "flag",
								Count: 3,
							},
						},
					},
				},
				"categories": rv.frs1["categories"],
			}

			return rv
		}(),
		func() *testCase {
			rv := &testCase{}

			rv.frs1 = FacetResults{
				"facetName": &FacetResult{
					Field:   "docField",
					Total:   0,
					Missing: 0,
					Other:   0,
					Terms:   nil,
				},
			}
			rv.frs2 = FacetResults{
				"facetName": &FacetResult{
					Field:   "docField",
					Total:   3,
					Missing: 0,
					Other:   0,
					Terms: &TermFacets{
						termFacets: []*TermFacet{
							{
								Term:  "firstTerm",
								Count: 1,
							},
							{
								Term:  "secondTerm",
								Count: 2,
							},
						},
					},
				},
			}
			rv.fixups = map[string]int{
				"facetName": 1,
			}

			rv.expFrs = FacetResults{
				"facetName": &FacetResult{
					Field:   "docField",
					Total:   3,
					Missing: 0,
					Other:   1,
					Terms: &TermFacets{
						termFacets: []*TermFacet{
							{
								Term:  "secondTerm",
								Count: 2,
							},
						},
					},
				},
			}
			return rv
		}(),
	}

	for tcIdx, tc := range tests {
		t.Run(fmt.Sprintf("T#%d", tcIdx), func(t *testing.T) {
			tc.frs1.Merge(tc.frs2)
			for facetName, size := range tc.fixups {
				tc.frs1.Fixup(facetName, size)
			}

			// clear termLookup, so we can compare the facet results
			for _, fr := range tc.frs1 {
				if fr.Terms != nil {
					fr.Terms.termLookup = nil
				}
			}

			if !reflect.DeepEqual(tc.frs1, tc.expFrs) {
				t.Errorf("expected %v, got %v", tc.expFrs, tc.frs1)
			}
		})
	}
}

func TestNumericFacetResultsMerge(t *testing.T) {

	lowmed := 3.0
	medhi := 6.0
	hihigher := 9.0

	// why second copy? the pointers may be different, but values the same
	lowmed2 := 3.0
	medhi2 := 6.0
	hihigher2 := 9.0

	fr1 := &FacetResult{
		Field:   "rating",
		Total:   100,
		Missing: 25,
		Other:   25,
		NumericRanges: []*NumericRangeFacet{
			{
				Name:  "low",
				Max:   &lowmed,
				Count: 25,
			},
			{
				Name:  "med",
				Count: 24,
				Max:   &lowmed,
				Min:   &medhi,
			},
			{
				Name:  "hi",
				Count: 1,
				Min:   &medhi,
				Max:   &hihigher,
			},
		},
	}
	frs1 := FacetResults{
		"ratings": fr1,
	}

	fr2 := &FacetResult{
		Field:   "rating",
		Total:   100,
		Missing: 25,
		Other:   25,
		NumericRanges: []*NumericRangeFacet{
			{
				Name:  "low",
				Max:   &lowmed2,
				Count: 25,
			},
			{
				Name:  "med",
				Max:   &lowmed2,
				Min:   &medhi2,
				Count: 22,
			},
			{
				Name:  "highest",
				Min:   &hihigher2,
				Count: 3,
			},
		},
	}
	frs2 := FacetResults{
		"ratings": fr2,
	}

	expectedFr := &FacetResult{
		Field:   "rating",
		Total:   200,
		Missing: 50,
		Other:   51,
		NumericRanges: []*NumericRangeFacet{
			{
				Name:  "low",
				Count: 50,
				Max:   &lowmed,
			},
			{
				Name:  "med",
				Max:   &lowmed,
				Min:   &medhi,
				Count: 46,
			},
			{
				Name:  "highest",
				Min:   &hihigher,
				Count: 3,
			},
		},
	}
	expectedFrs := FacetResults{
		"ratings": expectedFr,
	}

	frs1.Merge(frs2)
	frs1.Fixup("ratings", 3)
	if !reflect.DeepEqual(frs1, expectedFrs) {
		t.Errorf("expected %#v, got %#v", expectedFrs, frs1)
	}
}

func TestDateFacetResultsMerge(t *testing.T) {

	lowmed := "2010-01-01"
	medhi := "2011-01-01"
	hihigher := "2012-01-01"

	// why second copy? the pointer are to strings done by date time parsing
	// inside the facet generation, so comparing pointers will not work
	lowmed2 := "2010-01-01"
	medhi2 := "2011-01-01"
	hihigher2 := "2012-01-01"

	fr1 := &FacetResult{
		Field:   "birthday",
		Total:   100,
		Missing: 25,
		Other:   25,
		DateRanges: []*DateRangeFacet{
			{
				Name:  "low",
				End:   &lowmed,
				Count: 25,
			},
			{
				Name:  "med",
				Count: 24,
				Start: &lowmed,
				End:   &medhi,
			},
			{
				Name:  "hi",
				Count: 1,
				Start: &medhi,
				End:   &hihigher,
			},
		},
	}
	frs1 := FacetResults{
		"birthdays": fr1,
	}

	fr2 := &FacetResult{
		Field:   "birthday",
		Total:   100,
		Missing: 25,
		Other:   25,
		DateRanges: []*DateRangeFacet{
			{
				Name:  "low",
				End:   &lowmed2,
				Count: 25,
			},
			{
				Name:  "med",
				Start: &lowmed2,
				End:   &medhi2,
				Count: 22,
			},
			{
				Name:  "highest",
				Start: &hihigher2,
				Count: 3,
			},
		},
	}
	frs2 := FacetResults{
		"birthdays": fr2,
	}

	expectedFr := &FacetResult{
		Field:   "birthday",
		Total:   200,
		Missing: 50,
		Other:   51,
		DateRanges: []*DateRangeFacet{
			{
				Name:  "low",
				Count: 50,
				End:   &lowmed,
			},
			{
				Name:  "med",
				Start: &lowmed,
				End:   &medhi,
				Count: 46,
			},
			{
				Name:  "highest",
				Start: &hihigher,
				Count: 3,
			},
		},
	}
	expectedFrs := FacetResults{
		"birthdays": expectedFr,
	}

	frs1.Merge(frs2)
	frs1.Fixup("birthdays", 3)
	if !reflect.DeepEqual(frs1, expectedFrs) {
		t.Errorf("expected %#v, got %#v", expectedFrs, frs1)
	}
}
