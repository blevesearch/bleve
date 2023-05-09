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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	html_char_filter "github.com/blevesearch/bleve/v2/analysis/char/html"
	regexp_char_filter "github.com/blevesearch/bleve/v2/analysis/char/regexp"
	"github.com/blevesearch/bleve/v2/analysis/token/length"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/token/shingle"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/single"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"
	"github.com/blevesearch/bleve/v2/search/highlight/highlighter/html"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
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
					{Name: "testName", Start: time.Unix(0, 0), End: time.Now()},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Success_With_Start",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					{Name: "testName", Start: time.Unix(0, 0)},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Success_With_End",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					{Name: "testName", End: time.Now()},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_MinMax",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName", Min: &value, Max: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_Min",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName", Min: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Success_With_Max",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName", Max: &value},
				},
			},
			result: nil,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Missing_Failure",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					{Name: "testName2", Start: time.Unix(0, 0)},
					{Name: "testName1", End: time.Now()},
					{Name: "testName"},
				},
			},
			result: drMissingErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Missing_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName2", Min: &value},
					{Name: "testName1", Max: &value},
					{Name: "testName"},
				},
			},
			result: nrMissingErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_And_DateRanges_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName", Max: &value},
				},
				DateTimeRanges: []*dateTimeRange{
					{Name: "testName", End: time.Now()},
				},
			},
			result: drNrErr,
		},
		{
			facet: &FacetRequest{
				Field: "Numeric_Range_Name_Repeat_Failure",
				Size:  1,
				NumericRanges: []*numericRange{
					{Name: "testName", Min: &value},
					{Name: "testName", Max: &value},
				},
			},
			result: nrNameDupErr,
		},
		{
			facet: &FacetRequest{
				Field: "Date_Range_Name_Repeat_Failure",
				Size:  1,
				DateTimeRanges: []*dateTimeRange{
					{Name: "testName", Start: time.Unix(0, 0)},
					{Name: "testName", End: time.Now()},
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

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
				index.IndexField,
				&analysis.DefaultAnalyzer{
					Tokenizer: single.NewSingleTokenTokenizer(),
					TokenFilters: []analysis.TokenFilter{
						lowercase.NewLowerCaseFilter(),
					},
				},
			),
		}
		for k, v := range metadata {
			doc.AddField(document.NewTextFieldWithIndexingOptions(
				fmt.Sprintf("metadata.%s", k), []uint64{}, []byte(v), index.IndexField))
		}
		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"text"}, []string{},
				index.IndexField|index.IncludeTermVectors),
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
				index.IndexField|index.IncludeTermVectors),
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

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
			index.IndexField|index.IncludeTermVectors),
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
				index.IndexField|index.IncludeTermVectors),
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
			index.IndexField|index.IncludeTermVectors),
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	im := NewIndexMapping()
	idx, err := NewUsing(tmpIndexPath, im, indexName, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	// create an index with default settings
	idxMapping := NewIndexMapping()
	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
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
				index.IndexField|index.IncludeTermVectors),
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	of := NewTextFieldMapping()
	of.Analyzer = keyword.Name
	of.Name = "owner"

	dm := NewDocumentMapping()
	dm.AddFieldMappingsAt("owner", of)

	m := NewIndexMapping()
	m.DefaultMapping = dm

	idx, err := NewUsing(tmpIndexPath, m, "scorch", "scorch", nil)
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
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := NewUsing(tmpIndexPath, NewIndexMapping(), scorch.Name, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := idx.Close()
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

func TestGeoDistanceIssue1301(t *testing.T) {
	shopMapping := NewDocumentMapping()
	shopMapping.AddFieldMappingsAt("GEO", NewGeoPointFieldMapping())
	shopIndexMapping := NewIndexMapping()
	shopIndexMapping.DefaultMapping = shopMapping

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := NewUsing(tmpIndexPath, shopIndexMapping, scorch.Name, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	for i, g := range []string{"wecpkbeddsmf", "wecpk8tne453", "wecpkb80s09t"} {
		if err = idx.Index(strconv.Itoa(i), map[string]interface{}{
			"ID":  i,
			"GEO": g,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Not setting "Field" for the following query, targets it against the _all
	// field and this is returning inconsistent results, when there's another
	// field indexed along with the geopoint which is numeric.
	// As reported in: https://github.com/blevesearch/bleve/issues/1301
	lat, lon := 22.371154, 114.112603
	q := NewGeoDistanceQuery(lon, lat, "1km")

	req := NewSearchRequest(q)
	sr, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}

	if sr.Total != 3 {
		t.Fatalf("Size expected: 3, actual %d\n", sr.Total)
	}
}

func TestSearchHighlightingWithRegexpReplacement(t *testing.T) {
	idxMapping := NewIndexMapping()
	if err := idxMapping.AddCustomCharFilter(regexp_char_filter.Name, map[string]interface{}{
		"regexp":  `([a-z])\s+(\d)`,
		"replace": "ooooo$1-$2",
		"type":    regexp_char_filter.Name,
	}); err != nil {
		t.Fatal(err)
	}
	if err := idxMapping.AddCustomAnalyzer("regexp_replace", map[string]interface{}{
		"type":      custom.Name,
		"tokenizer": "unicode",
		"char_filters": []string{
			regexp_char_filter.Name,
		},
	}); err != nil {
		t.Fatal(err)
	}

	idxMapping.DefaultAnalyzer = "regexp_replace"
	idxMapping.StoreDynamic = true

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := NewUsing(tmpIndexPath, idxMapping, scorch.Name, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := map[string]interface{}{
		"status": "fool 10",
	}

	batch := idx.NewBatch()
	if err = batch.Index("doc", doc); err != nil {
		t.Fatal(err)
	}

	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	query := NewMatchQuery("fool 10")
	sreq := NewSearchRequest(query)
	sreq.Fields = []string{"*"}
	sreq.Highlight = NewHighlightWithStyle(ansi.Name)

	sres, err := idx.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}

	if sres.Total != 1 {
		t.Fatalf("Expected 1 hit, got: %v", sres.Total)
	}
}

func TestAnalyzerInheritance(t *testing.T) {
	tests := []struct {
		name       string
		mappingStr string
		doc        map[string]interface{}
		queryField string
		queryTerm  string
	}{
		{
			/*
				index_mapping: keyword
				default_mapping: ""
					-> child field (should inherit keyword)
			*/
			name: "Child field to inherit index mapping's default analyzer",
			mappingStr: `{"default_mapping":{"enabled":true,"dynamic":false,"properties":` +
				`{"city":{"enabled":true,"dynamic":false,"fields":[{"name":"city","type":"text",` +
				`"store":false,"index":true}]}}},"default_analyzer":"keyword"}`,
			doc:        map[string]interface{}{"city": "San Francisco"},
			queryField: "city",
			queryTerm:  "San Francisco",
		},
		{
			/*
				index_mapping: standard
				default_mapping: keyword
				    -> child field (should inherit keyword)
			*/
			name: "Child field to inherit default mapping's default analyzer",
			mappingStr: `{"default_mapping":{"enabled":true,"dynamic":false,"properties":` +
				`{"city":{"enabled":true,"dynamic":false,"fields":[{"name":"city","type":"text",` +
				`"index":true}]}},"default_analyzer":"keyword"},"default_analyzer":"standard"}`,
			doc:        map[string]interface{}{"city": "San Francisco"},
			queryField: "city",
			queryTerm:  "San Francisco",
		},
		{
			/*
				index_mapping: standard
				default_mapping: keyword (dynamic)
				    -> search over field to (should inherit keyword)
			*/
			name: "Child field to inherit default mapping's default analyzer",
			mappingStr: `{"default_mapping":{"enabled":true,"dynamic":true,"default_analyzer":"keyword"}` +
				`,"default_analyzer":"standard"}`,
			doc:        map[string]interface{}{"city": "San Francisco"},
			queryField: "city",
			queryTerm:  "San Francisco",
		},
		{
			/*
				index_mapping: standard
				default_mapping: keyword
				    -> child mapping: ""
					    -> child field: (should inherit keyword)
			*/
			name: "Nested child field to inherit default mapping's default analyzer",
			mappingStr: `{"default_mapping":{"enabled":true,"dynamic":false,"default_analyzer":` +
				`"keyword","properties":{"address":{"enabled":true,"dynamic":false,"properties":` +
				`{"city":{"enabled":true,"dynamic":false,"fields":[{"name":"city","type":"text",` +
				`"index":true}]}}}}},"default_analyzer":"standard"}`,
			doc: map[string]interface{}{
				"address": map[string]interface{}{"city": "San Francisco"},
			},
			queryField: "address.city",
			queryTerm:  "San Francisco",
		},
		{
			/*
				index_mapping: standard
				default_mapping: ""
				    -> child mapping: "keyword"
					    -> child mapping: ""
						    -> child field: (should inherit keyword)
			*/
			name: "Nested child field to inherit first child mapping's default analyzer",
			mappingStr: `{"default_mapping":{"enabled":true,"dynamic":false,"properties":` +
				`{"address":{"enabled":true,"dynamic":false,"default_analyzer":"keyword",` +
				`"properties":{"state":{"enabled":true,"dynamic":false,"properties":{"city":` +
				`{"enabled":true,"dynamic":false,"fields":[{"name":"city","type":"text",` +
				`"store":false,"index":true}]}}}}}}},"default_analyer":"standard"}`,
			doc: map[string]interface{}{
				"address": map[string]interface{}{
					"state": map[string]interface{}{"city": "San Francisco"},
				},
			},
			queryField: "address.state.city",
			queryTerm:  "San Francisco",
		},
	}

	for i := range tests {
		t.Run(fmt.Sprintf("%s", tests[i].name), func(t *testing.T) {
			idxMapping := NewIndexMapping()
			if err := idxMapping.UnmarshalJSON([]byte(tests[i].mappingStr)); err != nil {
				t.Fatal(err)
			}

			tmpIndexPath := createTmpIndexPath(t)
			idx, err := New(tmpIndexPath, idxMapping)
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				if err := idx.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if err = idx.Index("doc", tests[i].doc); err != nil {
				t.Fatal(err)
			}

			q := NewTermQuery(tests[i].queryTerm)
			q.SetField(tests[i].queryField)

			res, err := idx.Search(NewSearchRequest(q))
			if err != nil {
				t.Fatal(err)
			}

			if len(res.Hits) != 1 {
				t.Errorf("Unexpected number of hits: %v", len(res.Hits))
			}
		})
	}
}

func TestHightlightingWithHTMLCharacterFilter(t *testing.T) {
	idxMapping := NewIndexMapping()
	if err := idxMapping.AddCustomAnalyzer("custom-html", map[string]interface{}{
		"type":         custom.Name,
		"tokenizer":    "unicode",
		"char_filters": []interface{}{html_char_filter.Name},
	}); err != nil {
		t.Fatal(err)
	}

	fm := mapping.NewTextFieldMapping()
	fm.Analyzer = "custom-html"

	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("content", fm)

	idxMapping.DefaultMapping = dmap

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	content := "<div> Welcome to blevesearch. </div>"
	if err = idx.Index("doc", map[string]string{
		"content": content,
	}); err != nil {
		t.Fatal(err)
	}

	searchStr := "blevesearch"
	q := query.NewMatchQuery(searchStr)
	q.SetField("content")
	sr := NewSearchRequest(q)
	sr.IncludeLocations = true
	sr.Fields = []string{"*"}
	sr.Highlight = NewHighlightWithStyle(html.Name)
	searchResults, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if len(searchResults.Hits) != 1 ||
		len(searchResults.Hits[0].Locations["content"][searchStr]) != 1 {
		t.Fatalf("Expected 1 hit with 1 location")
	}

	expectedLocation := &search.Location{
		Pos:   3,
		Start: uint64(strings.Index(content, searchStr)),
		End:   uint64(strings.Index(content, searchStr) + len(searchStr)),
	}
	expectedFragment := "&lt;div&gt; Welcome to <mark>blevesearch</mark>. &lt;/div&gt;"

	gotLocation := searchResults.Hits[0].Locations["content"]["blevesearch"][0]
	gotFragment := searchResults.Hits[0].Fragments["content"][0]

	if !reflect.DeepEqual(expectedLocation, gotLocation) {
		t.Fatalf("Mismatch in locations, got: %v, expected: %v",
			gotLocation, expectedLocation)
	}

	if expectedFragment != gotFragment {
		t.Fatalf("Mismatch in fragment, got: %v, expected: %v",
			gotFragment, expectedFragment)
	}
}

func TestIPRangeQuery(t *testing.T) {
	idxMapping := NewIndexMapping()
	im := NewIPFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("ip_content", im)
	idxMapping.DefaultMapping = dmap

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	ipContent := "192.168.10.11"
	if err = idx.Index("doc", map[string]string{
		"ip_content": ipContent,
	}); err != nil {
		t.Fatal(err)
	}

	q := query.NewIPRangeQuery("192.168.10.0/24")
	q.SetField("ip_content")
	sr := NewSearchRequest(q)

	searchResults, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if len(searchResults.Hits) != 1 ||
		searchResults.Hits[0].ID != "doc" {
		t.Fatal("Expected the 1 result - doc")
	}
}

func TestGeoShapePolygonContainsPoint(t *testing.T) {
	fm := mapping.NewGeoShapeFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("geometry", fm)

	idxMapping := NewIndexMapping()
	idxMapping.DefaultMapping = dmap

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Polygon coordinates to be ordered in counter-clock-wise order
	// for the outer loop, and holes to follow clock-wise order.
	// See: https://www.rfc-editor.org/rfc/rfc7946.html#section-3.1.6

	one := []byte(`{
		"geometry":{
			"type":"Polygon",
			"coordinates":[[
				[4.8089,46.9307],
				[4.8223,46.8915],
				[4.8149,46.886],
				[4.8252,46.8647],
				[4.8305,46.8531],
				[4.8506,46.8509],
				[4.8574,46.8621],
				[4.8576,46.8769],
				[4.8753,46.8774],
				[4.8909,46.8519],
				[4.8837,46.8485],
				[4.9014,46.8318],
				[4.9067,46.8179],
				[4.8986,46.8122],
				[4.9081,46.7969],
				[4.9535,46.8254],
				[4.9577,46.8053],
				[5.0201,46.821],
				[5.0357,46.8207],
				[5.0656,46.8434],
				[5.0955,46.8411],
				[5.1149,46.8435],
				[5.1259,46.8395],
				[5.1433,46.8463],
				[5.1415,46.8589],
				[5.1533,46.873],
				[5.138,46.8843],
				[5.1525,46.9012],
				[5.1485,46.9165],
				[5.1582,46.926],
				[5.1882,46.9251],
				[5.2039,46.9129],
				[5.2223,46.9175],
				[5.2168,46.926],
				[5.2338,46.9316],
				[5.228,46.9505],
				[5.2078,46.9722],
				[5.2117,46.98],
				[5.1961,46.9783],
				[5.1663,46.9638],
				[5.1213,46.9634],
				[5.1086,46.9596],
				[5.0729,46.9604],
				[5.0731,46.9668],
				[5.0493,46.9817],
				[5.0034,46.9722],
				[4.9852,46.9585],
				[4.9479,46.9664],
				[4.8943,46.9663],
				[4.8937,46.951],
				[4.8534,46.9458],
				[4.8089,46.9307]
			]]
		}
	}`)

	two := []byte(`{
		"geometry":{
			"type":"Polygon",
			"coordinates":[[
				[2.2266,48.7816],
				[2.2266,48.7761],
				[2.2288,48.7745],
				[2.2717,48.7905],
				[2.2799,48.8109],
				[2.3013,48.8251],
				[2.2894,48.8283],
				[2.2726,48.8144],
				[2.2518,48.8164],
				[2.255,48.8101],
				[2.2348,48.7954],
				[2.2266,48.7816]
			]]
		}
	}`)

	var doc1, doc2 map[string]interface{}

	if err = json.Unmarshal(one, &doc1); err != nil {
		t.Fatal(err)
	}
	if err = idx.Index("doc1", doc1); err != nil {
		t.Fatal(err)
	}

	if err = json.Unmarshal(two, &doc2); err != nil {
		t.Fatal(err)
	}
	if err = idx.Index("doc2", doc2); err != nil {
		t.Fatal(err)
	}

	for testi, test := range []struct {
		coordinates []float64
		expectHits  []string
	}{
		{
			coordinates: []float64{5, 46.9},
			expectHits:  []string{"doc1"},
		},
		{
			coordinates: []float64{1.5, 48.2},
		},
	} {
		q, err := query.NewGeoShapeQuery(
			[][][][]float64{{{test.coordinates}}},
			geo.PointType,
			"contains",
		)
		if err != nil {
			t.Fatalf("test: %d, query err: %v", testi+1, err)
		}
		q.SetField("geometry")

		res, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatalf("test: %d, search err: %v", testi+1, err)
		}

		if len(res.Hits) != len(test.expectHits) {
			t.Errorf("test: %d, unexpected hits: %v", testi+1, len(res.Hits))
		}

	OUTER:
		for _, expect := range test.expectHits {
			for _, got := range res.Hits {
				if got.ID == expect {
					continue OUTER
				}
			}
			t.Errorf("test: %d, couldn't get: %v", testi+1, expect)
		}
	}
}

func TestAnalyzerInheritanceForDefaultDynamicMapping(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	imap := mapping.NewIndexMapping()
	imap.DefaultMapping.DefaultAnalyzer = keyword.Name

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := map[string]interface{}{
		"fieldX": "AbCdEf",
	}

	if err = idx.Index("doc", doc); err != nil {
		t.Fatal(err)
	}

	// Match query to apply keyword analyzer to fieldX.
	mq := NewMatchQuery("AbCdEf")
	mq.SetField("fieldX")

	sr := NewSearchRequest(mq)
	results, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}

	if len(results.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(results.Hits))
	}
}
