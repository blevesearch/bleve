package search

import (
	"reflect"
	"testing"
)

func TestFacetResultsMerge(t *testing.T) {

	fr1 := &FacetResult{
		Field:   "type",
		Total:   100,
		Missing: 25,
		Other:   25,
		Terms: []*TermFacet{
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
		},
	}
	fr1Only := &FacetResult{
		Field:   "category",
		Total:   97,
		Missing: 22,
		Other:   15,
		Terms: []*TermFacet{
			&TermFacet{
				Term:  "clothing",
				Count: 35,
			},
			&TermFacet{
				Term:  "electronics",
				Count: 25,
			},
		},
	}
	frs1 := FacetResults{
		"types":      fr1,
		"categories": fr1Only,
	}

	fr2 := &FacetResult{
		Field:   "type",
		Total:   100,
		Missing: 25,
		Other:   25,
		Terms: []*TermFacet{
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
		},
	}
	frs2 := FacetResults{
		"types": fr2,
	}

	expectedFr := &FacetResult{
		Field:   "type",
		Total:   200,
		Missing: 50,
		Other:   51,
		Terms: []*TermFacet{
			&TermFacet{
				Term:  "blog",
				Count: 50,
			},
			&TermFacet{
				Term:  "comment",
				Count: 46,
			},
			&TermFacet{
				Term:  "flag",
				Count: 3,
			},
		},
	}
	expectedFrs := FacetResults{
		"types":      expectedFr,
		"categories": fr1Only,
	}

	frs1.Merge(frs2)
	frs1.Fixup("types", 3)
	if !reflect.DeepEqual(frs1, expectedFrs) {
		t.Errorf("expected %v, got %v", expectedFrs, frs1)
	}
}
