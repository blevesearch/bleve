//  Copyright (c) 2013 Couchbase, Inc.
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
	"reflect"
	"testing"
)

func TestMergeLocations(t *testing.T) {
	flm1 := FieldTermLocationMap{
		"marty": TermLocationMap{
			"name": {
				&Location{
					Pos:   1,
					Start: 0,
					End:   5,
				},
			},
		},
	}

	flm2 := FieldTermLocationMap{
		"marty": TermLocationMap{
			"description": {
				&Location{
					Pos:   5,
					Start: 20,
					End:   25,
				},
			},
		},
	}

	flm3 := FieldTermLocationMap{
		"josh": TermLocationMap{
			"description": {
				&Location{
					Pos:   5,
					Start: 20,
					End:   25,
				},
			},
		},
	}

	expectedMerge := FieldTermLocationMap{
		"marty": TermLocationMap{
			"description": {
				&Location{
					Pos:   5,
					Start: 20,
					End:   25,
				},
			},
			"name": {
				&Location{
					Pos:   1,
					Start: 0,
					End:   5,
				},
			},
		},
		"josh": TermLocationMap{
			"description": {
				&Location{
					Pos:   5,
					Start: 20,
					End:   25,
				},
			},
		},
	}

	mergedLocations := MergeLocations([]FieldTermLocationMap{flm1, flm2, flm3})
	if !reflect.DeepEqual(expectedMerge, mergedLocations) {
		t.Errorf("expected %v, got %v", expectedMerge, mergedLocations)
	}
}


func TestMergeFieldTermLocations(t *testing.T) {
	ftls := []FieldTermLocation {
		FieldTermLocation{
			Field: "a",
			Term:  "a",
			Location: Location{
				Pos: 10,
				Start: 1,
				End: 2,
			},
		},
		FieldTermLocation{
			Field: "a",
			Term:  "b",
			Location: Location{
				Pos: 12,
				Start: 1,
				End: 2,
			},
		},
		FieldTermLocation{
			Field: "b",
			Term:  "a",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
			},
		},
		FieldTermLocation{
			Field: "a",
			Term:  "b",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
			},
		},
		FieldTermLocation{
			Field: "a",
			Term:  "a",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
			},
		},
		FieldTermLocation{
			Field: "e",
			Term:  "c",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
				ArrayPositions: []uint64{0,1},
			},
		},
		FieldTermLocation{
			Field: "e",
			Term:  "c",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
				ArrayPositions: []uint64{1,0},
			},
		},
		FieldTermLocation{
			Field: "a",
			Term:  "b",
			Location: Location{
				Pos: 1,
				Start: 1,
				End: 2,
			},
		},
	}
	dms := []*DocumentMatch {
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "a",
					Term:  "a",
					Location: Location{
						Pos: 10,
						Start: 1,
						End: 2,
					},
				},
			},
		},
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "a",
					Term:  "b",
					Location: Location{
						Pos: 12,
						Start: 1,
						End: 2,
					},
				},
			},
		},
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "b",
					Term:  "a",
					Location: Location{
						Pos: 1,
						Start: 1,
						End: 2,
					},
				},
			},
		},
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "a",
					Term:  "b",
					Location: Location{
						Pos: 1,
						Start: 1,
						End: 2,
					},
				},
			},
		},
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "a",
					Term:  "b",
					Location: Location{
						Pos: 1,
						Start: 1,
						End: 2,
					},
				},
			},
		},
		&DocumentMatch{
			FieldTermLocations: []FieldTermLocation {
				FieldTermLocation{
					Field: "a",
					Term:  "a",
					Location: Location{
						Pos: 1,
						Start: 1,
						End: 2,
					},
				},
			},
		},
	}
    expectedFtls := []FieldTermLocation {
	    FieldTermLocation{
		    Field: "a",
		    Term:  "a",
		    Location: Location{
			    Pos: 1,
			    Start: 1,
			    End: 2,
		    },
	    },
	    FieldTermLocation{
		    Field: "a",
		    Term:  "a",
		    Location: Location{
			    Pos: 10,
			    Start: 1,
			    End: 2,
		    },
	    },
	    FieldTermLocation{
		    Field: "a",
		    Term:  "b",
		    Location: Location{
			    Pos: 1,
			    Start: 1,
			    End: 2,
		    },
	    },
	    FieldTermLocation{
		    Field: "a",
		    Term:  "b",
		    Location: Location{
			    Pos: 12,
			    Start: 1,
			    End: 2,
		    },
	    },
	    FieldTermLocation{
		    Field: "b",
		    Term:  "a",
		    Location: Location{
			    Pos: 1,
			    Start: 1,
			    End: 2,
		    },
	    },
	    FieldTermLocation{
		    Field: "e",
		    Term:  "c",
		    Location: Location{
			    Pos: 1,
			    Start: 1,
			    End: 2,
			    ArrayPositions: []uint64{0,1},
		    },
	    },
	    FieldTermLocation{
		    Field: "e",
		    Term:  "c",
		    Location: Location{
			    Pos: 1,
			    Start: 1,
			    End: 2,
			    ArrayPositions: []uint64{1,0},
		    },
	    },

    }
	ftls = MergeFieldTermLocations(ftls, dms)
	if !reflect.DeepEqual(ftls, expectedFtls) {
		t.Errorf("expected %v, got %v", expectedFtls, ftls)
	}
}