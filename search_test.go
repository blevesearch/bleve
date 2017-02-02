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

package bleve

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/search"
)

func TestSearchResultString(t *testing.T) {

	tests := []struct {
		result *SearchResult
		str    string
	}{
		{
			result: &SearchResult{
				Request: &SearchRequest{
					Size: 10,
				},
				Total: 5,
				Took:  1 * time.Second,
				Hits: search.DocumentMatchCollection{
					&search.DocumentMatch{},
					&search.DocumentMatch{},
					&search.DocumentMatch{},
					&search.DocumentMatch{},
					&search.DocumentMatch{},
				},
			},
			str: "5 matches, showing 1 through 5, took 1s",
		},
		{
			result: &SearchResult{
				Request: &SearchRequest{
					Size: 0,
				},
				Total: 5,
				Hits:  search.DocumentMatchCollection{},
			},
			str: "5 matches",
		},
		{
			result: &SearchResult{
				Request: &SearchRequest{
					Size: 10,
				},
				Total: 0,
				Hits:  search.DocumentMatchCollection{},
			},
			str: "No matches",
		},
	}

	for _, test := range tests {
		srstring := test.result.String()
		if !strings.HasPrefix(srstring, test.str) {
			t.Errorf("expected to start %s, got %s", test.str, srstring)
		}
	}
}

func TestSearchResultMerge(t *testing.T) {
	l := &SearchResult{
		Status: &SearchStatus{
			Total:      1,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Total:    1,
		MaxScore: 1,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 1,
			},
		},
	}

	r := &SearchResult{
		Status: &SearchStatus{
			Total:      1,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Total:    1,
		MaxScore: 2,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "b",
				Score: 2,
			},
		},
	}

	expected := &SearchResult{
		Status: &SearchStatus{
			Total:      2,
			Successful: 2,
			Errors:     make(map[string]error),
		},
		Total:    2,
		MaxScore: 2,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 1,
			},
			&search.DocumentMatch{
				ID:    "b",
				Score: 2,
			},
		},
	}

	l.Merge(r)

	if !reflect.DeepEqual(l, expected) {
		t.Errorf("expected %#v, got %#v", expected, l)
	}
}

func TestUnmarshalingSearchResult(t *testing.T) {

	searchResponse := []byte(`{
    "status":{
      "total":1,
      "failed":1,
      "successful":0,
      "errors":{
        "default_index_362ce020b3d62b13_348f5c3c":"context deadline exceeded"
      }
    },
    "request":{
      "query":{
        "match":"emp",
        "field":"type",
        "boost":1,
        "prefix_length":0,
        "fuzziness":0
      },
    "size":10000000,
    "from":0,
    "highlight":null,
    "fields":[],
    "facets":null,
    "explain":false
  },
  "hits":null,
  "total_hits":0,
  "max_score":0,
  "took":0,
  "facets":null
}`)

	rv := &SearchResult{
		Status: &SearchStatus{
			Errors: make(map[string]error),
		},
	}
	err = json.Unmarshal(searchResponse, rv)
	if err != nil {
		t.Error(err)
	}
	if len(rv.Status.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(rv.Status.Errors))
	}
}

func verifyErrors(t *testing.T, actual, expected error) {
	//success case.
	if actual == nil && actual == expected {
		return
	}
	//actual error matches the expected error.
	if actual != nil && expected != nil && actual.Error() != expected.Error() {
		t.Error(actual)
	} else if actual == nil {
		t.Error("Expected -> " + expected.Error())
	}
}

func TestFacetNumericDateRangeRequests(t *testing.T) {
	var drMissingErr = fmt.Errorf("date range query must specify either start, end or both for range name 'testName'")
	var nrMissingErr = fmt.Errorf("numeric range query must specify either min, max or both for range name 'testName'")
	var drNrErr = fmt.Errorf("facet can only conain numeric ranges or date ranges, not both")
	var drNameDupErr = fmt.Errorf("date ranges contains duplicate name 'testName'")
	var nrNameDupErr = fmt.Errorf("numeric ranges contains duplicate name 'testName'")
	value := float64(5)

	facet := NewFacetRequest("Date_Range_Success_With_StartEnd", 1)
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", Start: time.Unix(0, 0), End: time.Now()})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Date_Range_Success_With_Start", 1)
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", Start: time.Unix(0, 0)})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Date_Range_Success_With_End", 1)
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", End: time.Now()})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Numeric_Range_Success_With_MinMax", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Min: &value, Max: &value})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Numeric_Range_Success_With_Min", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Min: &value})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Numeric_Range_Success_With_Max", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Max: &value})
	verifyErrors(t, facet.Validate(), nil)

	facet = NewFacetRequest("Date_Range_Missing_Failure", 1)
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName2", Start: time.Unix(0, 0)})
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName1", End: time.Now()})
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName"})
	verifyErrors(t, facet.Validate(), drMissingErr)

	facet = NewFacetRequest("Numeric_Range_Missing_Failure", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName2", Min: &value})
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName1", Max: &value})
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName"})
	verifyErrors(t, facet.Validate(), nrMissingErr)

	facet = NewFacetRequest("Numeric_And_DateRanges_Failure", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Min: &value, Max: nil})
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", Start: time.Unix(0, 0), End: time.Now()})
	verifyErrors(t, facet.Validate(), drNrErr)

	facet = NewFacetRequest("Date_Range_Name_Repeat_Failure", 1)
	facet.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", Start: time.Unix(0, 0)})
	facet.DateTimeRanges = append(facet.DateTimeRanges, &dateTimeRange{Name: "testName", End: time.Now()})
	verifyErrors(t, facet.Validate(), drNameDupErr)

	facet = NewFacetRequest("Numeric_Range_Name_Repeat_Failure", 1)
	facet.NumericRanges = make([]*numericRange, 0, 1)
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Min: &value})
	facet.NumericRanges = append(facet.NumericRanges, &numericRange{Name: "testName", Max: &value})
	verifyErrors(t, facet.Validate(), nrNameDupErr)

}
