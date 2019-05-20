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
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/analysis/token/length"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	"github.com/blevesearch/bleve/analysis/token/shingle"
	"github.com/blevesearch/bleve/analysis/tokenizer/single"
	"github.com/blevesearch/bleve/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index/scorch"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/highlight/highlighter/html"
	"github.com/blevesearch/bleve/search/query"
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

func TestFacetNumericDateRangeRequests(t *testing.T) {
	var drMissingErr = fmt.Errorf("date range query must specify either start, end or both for range name 'testName'")
	var nrMissingErr = fmt.Errorf("numeric range query must specify either min, max or both for range name 'testName'")
	var drNrErr = fmt.Errorf("facet can only conain numeric ranges or date ranges, not both")
	var drNameDupErr = fmt.Errorf("date ranges contains duplicate name 'testName'")
	var nrNameDupErr = fmt.Errorf("numeric ranges contains duplicate name 'testName'")
	value := float64(5)

	tests := []struct {
		facet  *FacetRequest
		result error
	}{
		{
			facet: &FacetRequest{
				Field: "Date_Range_Success_With_StartEnd",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName", Start: time.Unix(0, 0), End: time.Now()},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Success_With_Start",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName", Start: time.Unix(0, 0)},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Success_With_End",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName", End: time.Now()},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_MinMax",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName", Min: &value, Max: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_Min",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName", Min: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_Max",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName", Max: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Missing_Failure",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName2", Start: time.Unix(0, 0)},
					&dateTimeRange{Name: "testName1", End: time.Now()},
					&dateTimeRange{Name: "testName"},
				},
			},
			result: drMissingErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Missing_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName2", Min: &value},
					&numericRange{Name: "testName1", Max: &value},
					&numericRange{Name: "testName"},
				},
			},
			result: nrMissingErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_And_DateRanges_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName", Max: &value},
				},
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName", End: time.Now()},
				},
			},
			result: drNrErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Name_Repeat_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					&numericRange{Name: "testName", Min: &value},
					&numericRange{Name: "testName", Max: &value},
				},
			},
			result: nrNameDupErr,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Name_Repeat_Failure",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					&dateTimeRange{Name: "testName", Start: time.Unix(0, 0)},
					&dateTimeRange{Name: "testName", End: time.Now()},
				},
			},
			result: drNameDupErr,
		},
	}

	for _, test := range tests {
		result := test.facet.Validate()
		if !reflect.DeepEqual(result, test.result) {
			t.Errorf("expected %#v, got %#v", test.result, result)
		}
	}

}

func TestSearchResultFacetsMerge(t *testing.T) {
	lowmed := "2010-01-01"
	medhi := "2011-01-01"
	hihigher := "2012-01-01"

	fr := &search.FacetResult{
		Field:   "birthday",
		Total:   100,
		Missing: 25,
		Other:   25,
		DateRanges: []*search.DateRangeFacet{
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
	frs := search.FacetResults{
		"birthdays": fr,
	}

	l := &SearchResult{
		Status: &SearchStatus{
			Total:      10,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Total:    10,
		MaxScore: 1,
	}

	r := &SearchResult{
		Status: &SearchStatus{
			Total:      1,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Total:    1,
		MaxScore: 2,
		Facets:   frs,
	}

	expected := &SearchResult{
		Status: &SearchStatus{
			Total:      11,
			Successful: 2,
			Errors:     make(map[string]error),
		},
		Total:    11,
		MaxScore: 2,
		Facets:   frs,
	}

	l.Merge(r)

	if !reflect.DeepEqual(l, expected) {
		t.Errorf("expected %#v, got %#v", expected, l)
	}
}

func TestMemoryNeededForSearchResult(t *testing.T) {
	query := NewTermQuery("blah")
	req := NewSearchRequest(query)

	var sr SearchResult
	expect := sr.Size()
	var dm search.DocumentMatch
	expect += 10 * dm.Size()

	estimate := MemoryNeededForSearchResult(req)
	if estimate != uint64(expect) {
		t.Errorf("estimate not what is expected: %v != %v", estimate, expect)
	}
}

// https://github.com/blevesearch/bleve/issues/954
func TestNestedBooleanSearchers(t *testing.T) {
	// create an index with a custom analyzer
	idxMapping := NewIndexMapping()
	if err := idxMapping.AddCustomAnalyzer("3xbla", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{lowercase.Name, "stop_en"},
	}); err != nil {
		t.Fatal(err)
	}

	idxMapping.DefaultAnalyzer = "3xbla"
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	// create and insert documents as a batch
	batch := idx.NewBatch()
	matches := 0
	for i := 0; i < 100; i++ {
		hostname := fmt.Sprintf("planner_hostname_%d", i%5)
		metadata := map[string]string{"region": fmt.Sprintf("planner_us-east-%d", i%5)}

		// Expected matches
		if (hostname == "planner_hostname_1" || hostname == "planner_hostname_2") &&
			metadata["region"] == "planner_us-east-1" {
			matches++
		}

		doc := document.NewDocument(strconv.Itoa(i))
		doc.Fields = []document.Field{
			document.NewTextFieldCustom("hostname", []uint64{}, []byte(hostname),
				document.IndexField,
				&analysis.Analyzer{
					Tokenizer: single.NewSingleTokenTokenizer(),
					TokenFilters: []analysis.TokenFilter{
						lowercase.NewLowerCaseFilter(),
					},
				},
			),
		}
		for k, v := range metadata {
			doc.AddField(document.NewTextFieldWithIndexingOptions(
				fmt.Sprintf("metadata.%s", k), []uint64{}, []byte(v), document.IndexField))
		}
		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"text"}, []string{},
				document.IndexField|document.IncludeTermVectors),
		}

		if err = batch.IndexAdvanced(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	que, err := query.ParseQuery([]byte(
		`{
			"conjuncts": [
			{
				"must": {
					"conjuncts": [
					{
						"disjuncts": [
						{
							"match": "planner_hostname_1",
							"field": "hostname"
						},
						{
							"match": "planner_hostname_2",
							"field": "hostname"
						}
						]
					}
					]
				}
			},
			{
				"must": {
					"conjuncts": [
					{
						"match": "planner_us-east-1",
						"field": "metadata.region"
					}
					]
				}
			}
			]
		}`,
	))
	if err != nil {
		t.Fatal(err)
	}

	req := NewSearchRequest(que)
	req.Size = 100
	req.Fields = []string{"hostname", "metadata.region"}
	searchResults, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if matches != len(searchResults.Hits) {
		t.Fatalf("Unexpected result set, %v != %v", matches, len(searchResults.Hits))
	}
}

func TestNestedBooleanMustNotSearcherUpsidedown(t *testing.T) {
	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	// create and insert documents as a batch
	batch := idx.NewBatch()

	docs := []struct {
		id              string
		hasRole         bool
		investigationId string
	}{
		{
			id:              "1@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "1@2",
			hasRole:         false,
			investigationId: "2",
		},
		{
			id:              "2@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "2@2",
			hasRole:         false,
			investigationId: "2",
		},
		{
			id:              "3@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "3@2",
			hasRole:         false,
			investigationId: "2",
		},
		{
			id:              "4@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "5@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "6@1",
			hasRole:         true,
			investigationId: "1",
		},
		{
			id:              "7@1",
			hasRole:         true,
			investigationId: "1",
		},
	}

	for i := 0; i < len(docs); i++ {
		doc := document.NewDocument(docs[i].id)
		doc.Fields = []document.Field{
			document.NewTextField("id", []uint64{}, []byte(docs[i].id)),
			document.NewBooleanField("hasRole", []uint64{}, docs[i].hasRole),
			document.NewTextField("investigationId", []uint64{}, []byte(docs[i].investigationId)),
		}

		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"text"}, []string{},
				document.IndexField|document.IncludeTermVectors),
		}

		if err = batch.IndexAdvanced(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	tq := NewTermQuery("1")
	tq.SetField("investigationId")
	// using must not, for cases that the field did not exists at all
	hasRole := NewBoolFieldQuery(true)
	hasRole.SetField("hasRole")
	noRole := NewBooleanQuery()
	noRole.AddMustNot(hasRole)
	oneRolesOrNoRoles := NewBooleanQuery()
	oneRolesOrNoRoles.AddShould(noRole)
	oneRolesOrNoRoles.SetMinShould(1)
	q := NewConjunctionQuery(tq, oneRolesOrNoRoles)

	sr := NewSearchRequestOptions(q, 100, 0, false)
	sr.Fields = []string{"hasRole"}
	sr.Highlight = NewHighlight()

	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 0 {
		t.Fatalf("Unexpected result, %v != 0", res.Total)
	}
}

func TestSearchScorchOverEmptyKeyword(t *testing.T) {
	defaultIndexType := Config.DefaultIndexType
	Config.DefaultIndexType = scorch.Name

	dmap := mapping.NewDocumentMapping()
	dmap.DefaultAnalyzer = standard.Name

	fm := mapping.NewTextFieldMapping()
	fm.Analyzer = keyword.Name

	fm1 := mapping.NewTextFieldMapping()
	fm1.Analyzer = standard.Name

	dmap.AddFieldMappingsAt("id", fm)
	dmap.AddFieldMappingsAt("name", fm1)

	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap
	imap.DefaultAnalyzer = standard.Name

	idx, err := New("testidx", imap)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
		Config.DefaultIndexType = defaultIndexType
	}()

	for i := 0; i < 10; i++ {
		err = idx.Index(fmt.Sprint(i), map[string]string{"name": fmt.Sprintf("test%d", i), "id": ""})
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := idx.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 10 {
		t.Fatalf("Unexpected doc count: %v, expected 10", count)
	}

	q := query.NewWildcardQuery("test*")
	sr := NewSearchRequestOptions(q, 40, 0, false)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 10 {
		t.Fatalf("Unexpected search hits: %v, expected 10", res.Total)
	}
}

func TestMultipleNestedBooleanMustNotSearchersOnScorch(t *testing.T) {
	defaultIndexType := Config.DefaultIndexType
	Config.DefaultIndexType = scorch.Name

	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
		Config.DefaultIndexType = defaultIndexType
	}()

	// create and insert documents as a batch
	batch := idx.NewBatch()

	doc := document.NewDocument("1-child-0")
	doc.Fields = []document.Field{
		document.NewTextField("id", []uint64{}, []byte("1-child-0")),
		document.NewBooleanField("hasRole", []uint64{}, false),
		document.NewTextField("roles", []uint64{}, []byte("R1")),
		document.NewNumericField("type", []uint64{}, 0),
	}
	doc.CompositeFields = []*document.CompositeField{
		document.NewCompositeFieldWithIndexingOptions(
			"_all", true, []string{"text"}, []string{},
			document.IndexField|document.IncludeTermVectors),
	}

	if err = batch.IndexAdvanced(doc); err != nil {
		t.Fatal(err)
	}

	docs := []struct {
		id      string
		hasRole bool
		typ     int
	}{
		{
			id:      "16d6fa37-48fd-4dea-8b3d-a52bddf73951",
			hasRole: false,
			typ:     9,
		},
		{
			id:      "18fa9eb2-8b1f-46f0-8b56-b4c551213f78",
			hasRole: false,
			typ:     9,
		},
		{
			id:      "3085855b-d74b-474a-86c3-9bf3e4504382",
			hasRole: false,
			typ:     9,
		},
		{
			id:      "38ef5d28-0f85-4fb0-8a94-dd20751c3364",
			hasRole: false,
			typ:     9,
		},
	}

	for i := 0; i < len(docs); i++ {
		doc := document.NewDocument(docs[i].id)
		doc.Fields = []document.Field{
			document.NewTextField("id", []uint64{}, []byte(docs[i].id)),
			document.NewBooleanField("hasRole", []uint64{}, docs[i].hasRole),
			document.NewNumericField("type", []uint64{}, float64(docs[i].typ)),
		}

		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"text"}, []string{},
				document.IndexField|document.IncludeTermVectors),
		}

		if err = batch.IndexAdvanced(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	batch = idx.NewBatch()

	// Update 1st doc
	doc = document.NewDocument("1-child-0")
	doc.Fields = []document.Field{
		document.NewTextField("id", []uint64{}, []byte("1-child-0")),
		document.NewBooleanField("hasRole", []uint64{}, false),
		document.NewNumericField("type", []uint64{}, 0),
	}
	doc.CompositeFields = []*document.CompositeField{
		document.NewCompositeFieldWithIndexingOptions(
			"_all", true, []string{"text"}, []string{},
			document.IndexField|document.IncludeTermVectors),
	}

	if err = batch.IndexAdvanced(doc); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	inclusive := true
	val := float64(9)
	q := query.NewNumericRangeInclusiveQuery(&val, &val, &inclusive, &inclusive)
	q.SetField("type")
	initialQuery := query.NewBooleanQuery(nil, nil, []query.Query{q})

	// using must not, for cases that the field did not exists at all
	hasRole := NewBoolFieldQuery(true)
	hasRole.SetField("hasRole")
	noRole := NewBooleanQuery()
	noRole.AddMustNot(hasRole)

	rq := query.NewBooleanQuery([]query.Query{initialQuery, noRole}, nil, nil)

	sr := NewSearchRequestOptions(rq, 100, 0, false)
	sr.Fields = []string{"id", "hasRole", "type"}
	sr.Highlight = NewHighlight()

	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("Unexpected result, %v != 1", res.Total)
	}
}

func testBooleanMustNotSearcher(t *testing.T, indexName string) {
	im := NewIndexMapping()
	idx, err := NewUsing("testidx", im, indexName, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	docs := []struct {
		Name    string
		HasRole bool
	}{
		{
			Name: "13900",
		},
		{
			Name: "13901",
		},
		{
			Name: "13965",
		},
		{
			Name:    "13966",
			HasRole: true,
		},
		{
			Name:    "13967",
			HasRole: true,
		},
	}

	for _, doc := range docs {
		err := idx.Index(doc.Name, doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	lhs := NewDocIDQuery([]string{"13965", "13966", "13967"})
	hasRole := NewBoolFieldQuery(true)
	hasRole.SetField("HasRole")
	rhs := NewBooleanQuery()
	rhs.AddMustNot(hasRole)

	var compareLeftRightAndConjunction = func(idx Index, left, right query.Query) error {
		// left
		lr := NewSearchRequestOptions(left, 100, 0, false)
		lres, err := idx.Search(lr)
		if err != nil {
			return fmt.Errorf("error left: %v", err)
		}
		lresIds := map[string]struct{}{}
		for i := range lres.Hits {
			lresIds[lres.Hits[i].ID] = struct{}{}
		}
		// right
		rr := NewSearchRequestOptions(right, 100, 0, false)
		rres, err := idx.Search(rr)
		if err != nil {
			return fmt.Errorf("error right: %v", err)
		}
		rresIds := map[string]struct{}{}
		for i := range rres.Hits {
			rresIds[rres.Hits[i].ID] = struct{}{}
		}
		// conjunction
		cr := NewSearchRequestOptions(NewConjunctionQuery(left, right), 100, 0, false)
		cres, err := idx.Search(cr)
		if err != nil {
			return fmt.Errorf("error conjunction: %v", err)
		}
		for i := range cres.Hits {
			if _, ok := lresIds[cres.Hits[i].ID]; ok {
				if _, ok := rresIds[cres.Hits[i].ID]; !ok {
					return fmt.Errorf("error id %s missing from right", cres.Hits[i].ID)
				}
			} else {
				return fmt.Errorf("error id %s missing from left", cres.Hits[i].ID)
			}
		}
		return nil
	}

	err = compareLeftRightAndConjunction(idx, lhs, rhs)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBooleanMustNotSearcherUpsidedown(t *testing.T) {
	testBooleanMustNotSearcher(t, upsidedown.Name)
}

func TestBooleanMustNotSearcherScorch(t *testing.T) {
	testBooleanMustNotSearcher(t, scorch.Name)
}

func TestQueryStringEmptyConjunctionSearcher(t *testing.T) {
	mapping := NewIndexMapping()
	mapping.DefaultAnalyzer = keyword.Name
	index, err := NewMemOnly(mapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = index.Close()
	}()

	query := NewQueryStringQuery("foo:bar +baz:\"\"")
	searchReq := NewSearchRequest(query)

	_, _ = index.Search(searchReq)
}

func TestDisjunctionQueryIncorrectMin(t *testing.T) {
	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	// create and insert documents as a batch
	batch := idx.NewBatch()
	docs := []struct {
		field1 string
		field2 int
	}{
		{
			field1: "one",
			field2: 1,
		},
		{
			field1: "two",
			field2: 2,
		},
	}

	for i := 0; i < len(docs); i++ {
		doc := document.NewDocument(strconv.Itoa(docs[i].field2))
		doc.Fields = []document.Field{
			document.NewTextField("field1", []uint64{}, []byte(docs[i].field1)),
			document.NewNumericField("field2", []uint64{}, float64(docs[i].field2)),
		}
		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"text"}, []string{},
				document.IndexField|document.IncludeTermVectors),
		}
		if err = batch.IndexAdvanced(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	tq := NewTermQuery("one")
	dq := NewDisjunctionQuery(tq)
	dq.SetMin(2)
	sr := NewSearchRequestOptions(dq, 1, 0, false)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total > 0 {
		t.Fatalf("Expected 0 matches as disjunction query contains a single clause"+
			" but got: %v", res.Total)
	}
}

func TestBooleanShouldMinPropagation(t *testing.T) {
	idx, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc1 := map[string]interface{}{
		"dept": "queen",
		"name": "cersei lannister",
	}

	doc2 := map[string]interface{}{
		"dept": "kings guard",
		"name": "jaime lannister",
	}

	batch := idx.NewBatch()

	if err = batch.Index("doc1", doc1); err != nil {
		t.Fatal(err)
	}

	if err = batch.Index("doc2", doc2); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	// term dictionaries in the index for field..
	//  dept: queen kings guard
	//  name: cersei jaime lannister

	// the following match query would match doc2
	mq1 := NewMatchQuery("kings guard")
	mq1.SetField("dept")

	// the following match query would match both doc1 and doc2,
	// as both docs share common lastname
	mq2 := NewMatchQuery("jaime lannister")
	mq2.SetField("name")

	bq := NewBooleanQuery()
	bq.AddShould(mq1)
	bq.AddMust(mq2)

	sr := NewSearchRequest(bq)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 2 {
		t.Errorf("Expected 2 results, but got: %v", res.Total)
	}
}

func TestDisjunctionMinPropagation(t *testing.T) {
	idx, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc1 := map[string]interface{}{
		"dept": "finance",
		"name": "xyz",
	}

	doc2 := map[string]interface{}{
		"dept": "marketing",
		"name": "xyz",
	}

	doc3 := map[string]interface{}{
		"dept": "engineering",
		"name": "abc",
	}

	batch := idx.NewBatch()

	if err = batch.Index("doc1", doc1); err != nil {
		t.Fatal(err)
	}

	if err = batch.Index("doc2", doc2); err != nil {
		t.Fatal(err)
	}

	if err = batch.Index("doc3", doc3); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	mq1 := NewMatchQuery("finance")
	mq2 := NewMatchQuery("marketing")
	dq := NewDisjunctionQuery(mq1, mq2)
	dq.SetMin(3)

	dq2 := NewDisjunctionQuery(dq)
	dq2.SetMin(1)

	sr := NewSearchRequest(dq2)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 0 {
		t.Fatalf("Expect 0 results, but got: %v", res.Total)
	}
}

func TestDuplicateLocationsIssue1168(t *testing.T) {
	fm1 := NewTextFieldMapping()
	fm1.Analyzer = keyword.Name
	fm1.Name = "name1"

	dm := NewDocumentStaticMapping()
	dm.AddFieldMappingsAt("name", fm1)

	m := NewIndexMapping()
	m.DefaultMapping = dm

	idx, err := NewMemOnly(m)
	if err != nil {
		t.Fatalf("bleve new err: %v", err)
	}

	err = idx.Index("x", map[string]interface{}{
		"name": "marty",
	})
	if err != nil {
		t.Fatalf("bleve index err: %v", err)
	}

	q1 := NewTermQuery("marty")
	q2 := NewTermQuery("marty")
	dq := NewDisjunctionQuery(q1, q2)

	sreq := NewSearchRequest(dq)
	sreq.Fields = []string{"*"}
	sreq.Highlight = NewHighlightWithStyle(html.Name)

	sres, err := idx.Search(sreq)
	if err != nil {
		t.Fatalf("bleve search err: %v", err)
	}
	if len(sres.Hits[0].Locations["name1"]["marty"]) != 1 {
		t.Fatalf("duplicate marty")
	}
}

func TestBooleanMustSingleMatchNone(t *testing.T) {
	idxMapping := NewIndexMapping()
	if err := idxMapping.AddCustomTokenFilter(length.Name, map[string]interface{}{
		"min":  3.0,
		"max":  5.0,
		"type": length.Name,
	}); err != nil {
		t.Fatal(err)
	}
	if err := idxMapping.AddCustomAnalyzer("custom1", map[string]interface{}{
		"type":          "custom",
		"tokenizer":     "single",
		"token_filters": []interface{}{length.Name},
	}); err != nil {
		t.Fatal(err)
	}

	idxMapping.DefaultAnalyzer = "custom1"
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := map[string]interface{}{
		"languages_known": "Dutch",
		"dept":            "Sales",
	}

	batch := idx.NewBatch()
	if err = batch.Index("doc", doc); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	// this is a successful match
	matchSales := NewMatchQuery("Sales")
	matchSales.SetField("dept")

	// this would spin off a MatchNoneSearcher as the
	// token filter rules out the word "French"
	matchFrench := NewMatchQuery("French")
	matchFrench.SetField("languages_known")

	bq := NewBooleanQuery()
	bq.AddShould(matchSales)
	bq.AddMust(matchFrench)

	sr := NewSearchRequest(bq)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 0 {
		t.Fatalf("Expected 0 results but got: %v", res.Total)
	}
}

func TestBooleanMustNotSingleMatchNone(t *testing.T) {
	idxMapping := NewIndexMapping()
	if err := idxMapping.AddCustomTokenFilter(shingle.Name, map[string]interface{}{
		"min":  3.0,
		"max":  5.0,
		"type": shingle.Name,
	}); err != nil {
		t.Fatal(err)
	}
	if err := idxMapping.AddCustomAnalyzer("custom1", map[string]interface{}{
		"type":          "custom",
		"tokenizer":     "unicode",
		"token_filters": []interface{}{shingle.Name},
	}); err != nil {
		t.Fatal(err)
	}

	idxMapping.DefaultAnalyzer = "custom1"
	idx, err := New("testidx", idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := map[string]interface{}{
		"languages_known": "Dutch",
		"dept":            "Sales",
	}

	batch := idx.NewBatch()
	if err = batch.Index("doc", doc); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	// this is a successful match
	matchSales := NewMatchQuery("Sales")
	matchSales.SetField("dept")

	// this would spin off a MatchNoneSearcher as the
	// token filter rules out the word "Dutch"
	matchDutch := NewMatchQuery("Dutch")
	matchDutch.SetField("languages_known")

	matchEngineering := NewMatchQuery("Engineering")
	matchEngineering.SetField("dept")

	bq := NewBooleanQuery()
	bq.AddShould(matchSales)
	bq.AddMustNot(matchDutch, matchEngineering)

	sr := NewSearchRequest(bq)
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 0 {
		t.Fatalf("Expected 0 results but got: %v", res.Total)
	}
}

func TestBooleanSearchBug1185(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	of := NewTextFieldMapping()
	of.Analyzer = keyword.Name
	of.Name = "owner"

	dm := NewDocumentMapping()
	dm.AddFieldMappingsAt("owner", of)

	m := NewIndexMapping()
	m.DefaultMapping = dm

	idx, err := NewUsing("testidx", m, "scorch", "scorch", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = idx.Index("17112", map[string]interface{}{
		"owner": "marty",
		"type":  "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17139", map[string]interface{}{
		"type": "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("177777", map[string]interface{}{
		"type": "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("177778", map[string]interface{}{
		"type": "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17140", map[string]interface{}{
		"type": "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17000", map[string]interface{}{
		"owner": "marty",
		"type":  "x",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17141", map[string]interface{}{
		"type": "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17428", map[string]interface{}{
		"owner": "marty",
		"type":  "A Demo Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Index("17113", map[string]interface{}{
		"owner": "marty",
		"type":  "x",
	})
	if err != nil {
		t.Fatal(err)
	}

	matchTypeQ := NewMatchPhraseQuery("A Demo Type")
	matchTypeQ.SetField("type")

	matchAnyOwnerRegQ := NewRegexpQuery(".+")
	matchAnyOwnerRegQ.SetField("owner")

	matchNoOwner := NewBooleanQuery()
	matchNoOwner.AddMustNot(matchAnyOwnerRegQ)

	notNoOwner := NewBooleanQuery()
	notNoOwner.AddMustNot(matchNoOwner)

	matchTypeAndNoOwner := NewConjunctionQuery()
	matchTypeAndNoOwner.AddQuery(matchTypeQ)
	matchTypeAndNoOwner.AddQuery(notNoOwner)

	req := NewSearchRequest(matchTypeAndNoOwner)
	res, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}

	// query 2
	matchTypeAndNoOwnerBoolean := NewBooleanQuery()
	matchTypeAndNoOwnerBoolean.AddMust(matchTypeQ)
	matchTypeAndNoOwnerBoolean.AddMustNot(matchNoOwner)

	req2 := NewSearchRequest(matchTypeAndNoOwnerBoolean)
	res2, err := idx.Search(req2)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Hits) != len(res2.Hits) {
		t.Fatalf("expected same number of hits, got: %d and %d", len(res.Hits), len(res2.Hits))
	}
}

func TestSearchScoreNone(t *testing.T) {
	idx, err := NewUsing("testidx", NewIndexMapping(), scorch.Name, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := map[string]interface{}{
		"field1": "asd fgh jkl",
		"field2": "more content blah blah",
		"id":     "doc",
	}

	if err = idx.Index("doc", doc); err != nil {
		t.Fatal(err)
	}

	q := NewQueryStringQuery("content")
	sr := NewSearchRequest(q)
	sr.IncludeLocations = true
	sr.Score = "none"

	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Hits) != 1 {
		t.Fatal("unexpected number of hits")
	}

	if len(res.Hits[0].Locations) != 1 {
		t.Fatal("unexpected locations for the hit")
	}

	if res.Hits[0].Score != 0 {
		t.Fatal("unexpected score for the hit")
	}
}
