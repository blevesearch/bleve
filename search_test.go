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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	html_char_filter "github.com/blevesearch/bleve/v2/analysis/char/html"
	regexp_char_filter "github.com/blevesearch/bleve/v2/analysis/char/regexp"
	"github.com/blevesearch/bleve/v2/analysis/datetime/flexible"
	"github.com/blevesearch/bleve/v2/analysis/datetime/iso"
	"github.com/blevesearch/bleve/v2/analysis/datetime/percent"
	"github.com/blevesearch/bleve/v2/analysis/datetime/sanitized"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/microseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/milliseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/nanoseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/seconds"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
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

func TestSortedFacetedQuery(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMapping := NewIndexMapping()
	indexMapping.TypeField = "type"
	indexMapping.DefaultAnalyzer = "en"
	documentMapping := NewDocumentMapping()
	indexMapping.AddDocumentMapping("hotel", documentMapping)

	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Index = true
	contentFieldMapping.DocValues = true
	documentMapping.AddFieldMappingsAt("content", contentFieldMapping)
	documentMapping.AddFieldMappingsAt("country", contentFieldMapping)

	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if err := index.Index("1", map[string]interface{}{
		"country": "india",
		"content": "k",
	}); err != nil {
		t.Fatal(err)
	}

	if err := index.Index("2", map[string]interface{}{
		"country": "india",
		"content": "l",
	}); err != nil {
		t.Fatal(err)
	}

	if err := index.Index("3", map[string]interface{}{
		"country": "india",
		"content": "k",
	}); err != nil {
		t.Fatal(err)
	}

	d, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	if d != 3 {
		t.Errorf("expected 3, got %d", d)
	}

	query := NewMatchPhraseQuery("india")
	query.SetField("country")
	searchRequest := NewSearchRequest(query)
	searchRequest.SortBy([]string{"content"})
	fr := NewFacetRequest("content", 100)
	searchRequest.AddFacet("content_facet", fr)

	searchResults, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedResults := map[string]int{"k": 2, "l": 1}

	for _, v := range searchResults.Facets {
		for _, v1 := range v.Terms.Terms() {
			if v1.Count != expectedResults[v1.Term] {
				t.Errorf("expected %d, got %d", expectedResults[v1.Term], v1.Count)
			}
		}
	}
}

func TestMatchAllScorer(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMapping := NewIndexMapping()
	indexMapping.TypeField = "type"
	indexMapping.DefaultAnalyzer = "en"
	documentMapping := NewDocumentMapping()

	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Index = true
	contentFieldMapping.Store = true
	documentMapping.AddFieldMappingsAt("content", contentFieldMapping)

	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if err := index.Index("1", map[string]interface{}{
		"country": "india",
		"content": "k",
	}); err != nil {
		t.Fatal(err)
	}

	if err := index.Index("2", map[string]interface{}{
		"country": "india",
		"content": "l",
	}); err != nil {
		t.Fatal(err)
	}

	if err := index.Index("3", map[string]interface{}{
		"country": "india",
		"content": "k",
	}); err != nil {
		t.Fatal(err)
	}

	d, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if d != 3 {
		t.Errorf("expected 3, got %d", d)
	}

	searchRequest := NewSearchRequest(NewMatchAllQuery())
	searchRequest.Score = "none"
	searchResults, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	if searchResults.Total != 3 {
		t.Fatalf("expected all the 3 docs in the index, got %v", searchResults.Total)
	}

	for _, hit := range searchResults.Hits {
		if hit.Score != 0.0 {
			t.Fatalf("expected 0 score since score = none, got %v", hit.Score)
		}
	}
}

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
	drMissingErr := fmt.Errorf("date range query must specify either start, end or both for range name 'testName'")
	nrMissingErr := fmt.Errorf("numeric range query must specify either min, max or both for range name 'testName'")
	drNrErr := fmt.Errorf("facet can only contain numeric ranges or date ranges, not both")
	drNameDupErr := fmt.Errorf("date ranges contains duplicate name 'testName'")
	nrNameDupErr := fmt.Errorf("numeric ranges contains duplicate name 'testName'")
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

	compareLeftRightAndConjunction := func(idx Index, left, right query.Query) error {
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

func TestMatchQueryPartialMatch(t *testing.T) {
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
		"description": "Patrick is first name Stewart is last name",
	}
	doc2 := map[string]interface{}{
		"description": "Manager given name is Patrick",
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
	// Test 1 - Both Docs hit, doc 1 = Full Match and doc 2 = Partial Match
	mq1 := NewMatchQuery("patrick stewart")
	mq1.SetField("description")

	sr := NewSearchRequest(mq1)
	sr.Explain = true
	res, err := idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 {
		t.Errorf("Expected 2 results, but got: %v", res.Total)
	}
	for _, hit := range res.Hits {
		switch hit.ID {
		case "doc1":
			if hit.Expl.PartialMatch {
				t.Errorf("Expected doc1 to be a full match")
			}
		case "doc2":
			if !hit.Expl.PartialMatch {
				t.Errorf("Expected doc2 to be a partial match")
			}
		default:
			t.Errorf("Unexpected document ID: %s", hit.ID)
		}
	}

	// Test 2 - Both Docs hit, doc 1 = Partial Match and doc 2 = Full Match
	mq2 := NewMatchQuery("paltric manner")
	mq2.SetField("description")
	mq2.SetFuzziness(2)

	sr = NewSearchRequest(mq2)
	sr.Explain = true
	res, err = idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 {
		t.Errorf("Expected 2 results, but got: %v", res.Total)
	}
	for _, hit := range res.Hits {
		switch hit.ID {
		case "doc1":
			if !hit.Expl.PartialMatch {
				t.Errorf("Expected doc1 to be a partial match")
			}
		case "doc2":
			if hit.Expl.PartialMatch {
				t.Errorf("Expected doc2 to be a full match")
			}
		default:
			t.Errorf("Unexpected document ID: %s", hit.ID)
		}
	}
	// Test 3 - Two Docs hits, both full match
	mq3 := NewMatchQuery("patrick")
	mq3.SetField("description")

	sr = NewSearchRequest(mq3)
	sr.Explain = true
	res, err = idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 {
		t.Errorf("Expected 2 results, but got: %v", res.Total)
	}
	for _, hit := range res.Hits {
		switch hit.ID {
		case "doc1":
			if hit.Expl.PartialMatch {
				t.Errorf("Expected doc1 to be a full match")
			}
		case "doc2":
			if hit.Expl.PartialMatch {
				t.Errorf("Expected doc2 to be a full match")
			}
		default:
			t.Errorf("Unexpected document ID: %s", hit.ID)
		}
	}
	// Test 4 - Two Docs hits, both partial match
	mq4 := NewMatchQuery("patrick stewart manager")
	mq4.SetField("description")

	sr = NewSearchRequest(mq4)
	sr.Explain = true
	res, err = idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 {
		t.Errorf("Expected 2 results, but got: %v", res.Total)
	}
	for _, hit := range res.Hits {
		switch hit.ID {
		case "doc1":
			if !hit.Expl.PartialMatch {
				t.Errorf("Expected doc1 to be a full match")
			}
		case "doc2":
			if !hit.Expl.PartialMatch {
				t.Errorf("Expected doc2 to be a full match")
			}
		default:
			t.Errorf("Unexpected document ID: %s", hit.ID)
		}
	}

	// Test 5 - Match Query AND operator always results in full match
	mq5 := NewMatchQuery("patrick stewart")
	mq5.SetField("description")
	mq5.SetOperator(1)

	sr = NewSearchRequest(mq5)
	sr.Explain = true
	res, err = idx.Search(sr)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 {
		t.Errorf("Expected 1 result, but got: %v", res.Total)
	}
	hit := res.Hits[0]
	if hit.ID != "doc1" || hit.Expl.PartialMatch {
		t.Errorf("Expected doc1 to be a full match")
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
		t.Run(tests[i].name, func(t *testing.T) {
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
		q, err := NewGeoShapeQuery(
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

func TestCustomDateTimeParserLayoutValidation(t *testing.T) {
	flexiblegoName := flexible.Name
	sanitizedgoName := sanitized.Name
	imap := mapping.NewIndexMapping()
	correctConfig := map[string]interface{}{
		"type": sanitizedgoName,
		"layouts": []interface{}{
			// some custom layouts
			"2006-01-02 15:04:05.0000",
			"2006\\01\\02T03:04:05PM",
			"2006/01/02",
			"2006-01-02T15:04:05.999Z0700PMMST",
			"15:04:05.0000Z07:00 Monday",

			// standard layouts
			time.Layout,
			time.ANSIC,
			time.UnixDate,
			time.RubyDate,
			time.RFC822,
			time.RFC822Z,
			time.RFC850,
			time.RFC1123,
			time.RFC1123Z,
			time.RFC3339,
			time.RFC3339Nano,
			time.Kitchen,
			time.Stamp,
			time.StampMilli,
			time.StampMicro,
			time.StampNano,
			"2006-01-02 15:04:05", // time.DateTime
			"2006-01-02",          // time.DateOnly
			"15:04:05",            // time.TimeOnly

			// Corrected layouts to the incorrect ones below.
			"2006-01-02 03:04:05 -0700",
			"2006-01-02 15:04:05 -0700",
			"3:04PM",
			"2006-01-02 15:04:05.000 -0700 MST",
			"January 2 2006 3:04 PM",
			"02/Jan/06 3:04PM",
			"Mon 02 Jan 3:04:05 PM",
		},
	}

	// Correct layouts - sanitizedgo should work without errors.
	err := imap.AddCustomDateTimeParser("custDT", correctConfig)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// Flexiblego should work without errors as well.
	correctConfig["type"] = flexiblegoName
	err = imap.AddCustomDateTimeParser("custDT_Flexi", correctConfig)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	incorrectLayouts := [][]interface{}{
		{
			"2000-03-31 01:33:51 +0300",
		},
		{
			"2006-01-02 15:04:51 +0300",
		},
		{
			"2000-03-31 01:33:05 +0300",
		},
		{
			"4:45PM",
		},
		{
			"2006-01-02 15:04:05.445 -0700 MST",
		},
		{
			"August 20 2001 8:55 AM",
		},
		{
			"28/Jul/23 12:48PM",
		},
		{
			"Tue 22 Aug 6:37:30 AM",
		},
	}

	// first check sanitizedgo, should throw error for each of the incorrect layouts.
	numExpectedErrors := len(incorrectLayouts)
	numActualErrors := 0
	for idx, badLayout := range incorrectLayouts {
		incorrectConfig := map[string]interface{}{
			"type":    sanitizedgoName,
			"layouts": badLayout,
		}
		err := imap.AddCustomDateTimeParser(fmt.Sprintf("%d_DT", idx), incorrectConfig)
		if err != nil {
			numActualErrors++
		}
	}
	// Expecting all layouts to be incorrect, since sanitizedgo is being used.
	if numActualErrors != numExpectedErrors {
		t.Fatalf("expected %d errors, got: %d", numExpectedErrors, numActualErrors)
	}

	// sanity test - flexiblego should still allow the incorrect layouts, for legacy purposes
	for idx, badLayout := range incorrectLayouts {
		incorrectConfig := map[string]interface{}{
			"type":    flexiblegoName,
			"layouts": badLayout,
		}
		err := imap.AddCustomDateTimeParser(fmt.Sprintf("%d_DT_Flexi", idx), incorrectConfig)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	}
}

func TestDateRangeStringQuery(t *testing.T) {
	idxMapping := NewIndexMapping()

	err := idxMapping.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idxMapping.AddCustomDateTimeParser("queryDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dtmap := NewDateTimeFieldMapping()
	dtmap.DateFormat = "customDT"
	idxMapping.DefaultMapping.AddFieldMappingsAt("date", dtmap)

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
	documents := map[string]map[string]interface{}{
		"doc1": {
			"date": "2001/08/20 6:00PM",
		},
		"doc2": {
			"date": "20/08/2001 18:00:20",
		},
		"doc3": {
			"date": "20/08/2001 18:10:00",
		},
		"doc4": {
			"date": "2001/08/20 6:15PM",
		},
		"doc5": {
			"date": "20/08/2001 18:20:00",
		},
	}

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	type testResult struct {
		docID    string // doc ID of the hit
		hitField string // fields returned as part of the hit
	}

	type testStruct struct {
		start          string
		end            string
		field          string
		dateTimeParser string // name of the custom date time parser to use if nil, use QueryDateTimeParser
		includeStart   bool
		includeEnd     bool
		expectedHits   []testResult
		err            error
	}

	testQueries := []testStruct{
		// test cases with RFC3339 parser and toggling includeStart and includeEnd
		{
			start:        "2001-08-20T18:00:00",
			end:          "2001-08-20T18:10:00",
			field:        "date",
			includeStart: true,
			includeEnd:   true,
			expectedHits: []testResult{
				{
					docID:    "doc1",
					hitField: "2001/08/20 6:00PM",
				},
				{
					docID:    "doc2",
					hitField: "20/08/2001 18:00:20",
				},
				{
					docID:    "doc3",
					hitField: "20/08/2001 18:10:00",
				},
			},
		},
		{
			start:        "2001-08-20T18:00:00",
			end:          "2001-08-20T18:10:00",
			field:        "date",
			includeStart: false,
			includeEnd:   true,
			expectedHits: []testResult{
				{
					docID:    "doc2",
					hitField: "20/08/2001 18:00:20",
				},
				{
					docID:    "doc3",
					hitField: "20/08/2001 18:10:00",
				},
			},
		},
		{
			start:        "2001-08-20T18:00:00",
			end:          "2001-08-20T18:10:00",
			field:        "date",
			includeStart: false,
			includeEnd:   false,
			expectedHits: []testResult{
				{
					docID:    "doc2",
					hitField: "20/08/2001 18:00:20",
				},
			},
		},
		// test cases with custom parser and omitting start and end
		{
			start:          "20/08/2001 18:00:00",
			end:            "2001/08/20 6:10PM",
			field:          "date",
			dateTimeParser: "customDT",
			includeStart:   true,
			includeEnd:     true,
			expectedHits: []testResult{
				{
					docID:    "doc1",
					hitField: "2001/08/20 6:00PM",
				},
				{
					docID:    "doc2",
					hitField: "20/08/2001 18:00:20",
				},
				{
					docID:    "doc3",
					hitField: "20/08/2001 18:10:00",
				},
			},
		},
		{
			end:            "20/08/2001 18:15:00",
			field:          "date",
			dateTimeParser: "customDT",
			includeStart:   true,
			includeEnd:     true,
			expectedHits: []testResult{
				{
					docID:    "doc1",
					hitField: "2001/08/20 6:00PM",
				},
				{
					docID:    "doc2",
					hitField: "20/08/2001 18:00:20",
				},
				{
					docID:    "doc3",
					hitField: "20/08/2001 18:10:00",
				},
				{
					docID:    "doc4",
					hitField: "2001/08/20 6:15PM",
				},
			},
		},
		{
			start:          "2001/08/20 6:15PM",
			field:          "date",
			dateTimeParser: "customDT",
			includeStart:   true,
			includeEnd:     true,
			expectedHits: []testResult{
				{
					docID:    "doc4",
					hitField: "2001/08/20 6:15PM",
				},
				{
					docID:    "doc5",
					hitField: "20/08/2001 18:20:00",
				},
			},
		},
		{
			start:          "20/08/2001 6:15PM",
			field:          "date",
			dateTimeParser: "queryDT",
			includeStart:   true,
			includeEnd:     true,
			expectedHits: []testResult{
				{
					docID:    "doc4",
					hitField: "2001/08/20 6:15PM",
				},
				{
					docID:    "doc5",
					hitField: "20/08/2001 18:20:00",
				},
			},
		},
		// error path test cases
		{
			field:          "date",
			dateTimeParser: "customDT",
			includeStart:   true,
			includeEnd:     true,
			err:            fmt.Errorf("date range query must specify at least one of start/end"),
		},
		{
			field:        "date",
			includeStart: true,
			includeEnd:   true,
			err:          fmt.Errorf("date range query must specify at least one of start/end"),
		},
		{
			start:          "2001-08-20T18:00:00",
			end:            "2001-08-20T18:10:00",
			field:          "date",
			dateTimeParser: "customDT",
			err:            fmt.Errorf("unable to parse datetime with any of the layouts, date time parser name: customDT"),
		},
		{
			start: "3001-08-20T18:00:00",
			end:   "2001-08-20T18:10:00",
			field: "date",
			err:   fmt.Errorf("invalid/unsupported date range, start: 3001-08-20T18:00:00"),
		},
		{
			start:          "2001/08/20 6:00PM",
			end:            "3001/08/20 6:30PM",
			field:          "date",
			dateTimeParser: "customDT",
			err:            fmt.Errorf("invalid/unsupported date range, end: 3001/08/20 6:30PM"),
		},
	}

	for _, dtq := range testQueries {
		var err error
		dateQuery := NewDateRangeInclusiveStringQuery(dtq.start, dtq.end, &dtq.includeStart, &dtq.includeEnd)
		dateQuery.SetDateTimeParser(dtq.dateTimeParser)
		dateQuery.SetField(dtq.field)

		sr := NewSearchRequest(dateQuery)
		sr.SortBy([]string{dtq.field})
		sr.Fields = []string{dtq.field}

		res, err := idx.Search(sr)
		if err != nil {
			if dtq.err == nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if dtq.err.Error() != err.Error() {
				t.Fatalf("expected error: %v, got: %v", dtq.err, err)
			}
			continue
		}
		if len(res.Hits) != len(dtq.expectedHits) {
			t.Fatalf("expected %d hits, got %d", len(dtq.expectedHits), len(res.Hits))
		}
		for i, hit := range res.Hits {
			if hit.ID != dtq.expectedHits[i].docID {
				t.Fatalf("expected docID %s, got %s", dtq.expectedHits[i].docID, hit.ID)
			}
			if hit.Fields[dtq.field].(string) != dtq.expectedHits[i].hitField {
				t.Fatalf("expected hit field %s, got %s", dtq.expectedHits[i].hitField, hit.Fields[dtq.field])
			}
		}
	}
}

func TestDateRangeFacetQueriesWithCustomDateTimeParser(t *testing.T) {
	idxMapping := NewIndexMapping()

	err := idxMapping.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = idxMapping.AddCustomDateTimeParser("queryDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dtmap := NewDateTimeFieldMapping()
	dtmap.DateFormat = "customDT"
	idxMapping.DefaultMapping.AddFieldMappingsAt("date", dtmap)

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
	documents := map[string]map[string]interface{}{
		"doc1": {
			"date": "2001/08/20 6:00PM",
		},
		"doc2": {
			"date": "20/08/2001 18:00:20",
		},
		"doc3": {
			"date": "20/08/2001 18:10:00",
		},
		"doc4": {
			"date": "2001/08/20 6:15PM",
		},
		"doc5": {
			"date": "20/08/2001 18:20:00",
		},
	}

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	query := NewMatchAllQuery()

	type testFacetResult struct {
		name  string
		start string
		end   string
		count int
		err   error
	}

	type testFacetRequest struct {
		name   string
		start  string
		end    string
		parser string
		result testFacetResult
	}

	tests := []testFacetRequest{
		{
			// Test without a query time override of the parser (use default parser)
			name:  "test",
			start: "2001-08-20 18:00:00",
			end:   "2001-08-20 18:10:00",
			result: testFacetResult{
				name:  "test",
				start: "2001-08-20T18:00:00Z",
				end:   "2001-08-20T18:10:00Z",
				count: 2,
				err:   nil,
			},
		},
		{
			name:   "test",
			start:  "20/08/2001 6:00PM",
			end:    "20/08/2001 6:10PM",
			parser: "queryDT",
			result: testFacetResult{
				name:  "test",
				start: "2001-08-20T18:00:00Z",
				end:   "2001-08-20T18:10:00Z",
				count: 2,
				err:   nil,
			},
		},
		{
			name:   "test",
			start:  "20/08/2001 15:00:00",
			end:    "2001/08/20 6:10PM",
			parser: "customDT",
			result: testFacetResult{
				name:  "test",
				start: "2001-08-20T15:00:00Z",
				end:   "2001-08-20T18:10:00Z",
				count: 2,
				err:   nil,
			},
		},
		{
			name:   "test",
			end:    "2001/08/20 6:15PM",
			parser: "customDT",
			result: testFacetResult{
				name:  "test",
				end:   "2001-08-20T18:15:00Z",
				count: 3,
				err:   nil,
			},
		},
		{
			name:   "test",
			start:  "20/08/2001 6:15PM",
			parser: "queryDT",
			result: testFacetResult{
				name:  "test",
				start: "2001-08-20T18:15:00Z",
				count: 2,
				err:   nil,
			},
		},
		// some error cases
		{
			name:   "test",
			parser: "queryDT",
			result: testFacetResult{
				name: "test",
				err:  fmt.Errorf("date range query must specify either start, end or both for date range name 'test'"),
			},
		},
		{
			// default parser is used for the query, but the start time is not in the correct format (RFC3339),
			// so it should throw an error
			name:  "test",
			start: "20/08/2001 6:15PM",
			result: testFacetResult{
				name: "test",
				err:  fmt.Errorf("ParseDates err: error parsing start date '20/08/2001 6:15PM' for date range name 'test': unable to parse datetime with any of the layouts, using date time parser named dateTimeOptional"),
			},
		},
	}

	for _, test := range tests {
		searchRequest := NewSearchRequest(query)

		fr := NewFacetRequest("date", 100)
		start := &test.start
		if test.start == "" {
			start = nil
		}
		end := &test.end
		if test.end == "" {
			end = nil
		}

		fr.AddDateTimeRangeStringWithParser(test.name, start, end, test.parser)
		searchRequest.AddFacet("dateFacet", fr)

		searchResults, err := idx.Search(searchRequest)
		if err != nil {
			if test.result.err == nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err.Error() != test.result.err.Error() {
				t.Fatalf("Expected error %v, got %v", test.result.err, err)
			}
			continue
		}
		for _, facetResult := range searchResults.Facets {
			if len(facetResult.DateRanges) != 1 {
				t.Fatal("Expected 1 date range facet")
			}
			result := facetResult.DateRanges[0]
			if result.Name != test.result.name {
				t.Fatalf("Expected name %s, got %s", test.result.name, result.Name)
			}
			if result.Start != nil && *result.Start != test.result.start {
				t.Fatalf("Expected start %s, got %s", test.result.start, *result.Start)
			}
			if result.End != nil && *result.End != test.result.end {
				t.Fatalf("Expected end %s, got %s", test.result.end, *result.End)
			}
			if result.Start == nil && test.result.start != "" {
				t.Fatalf("Expected start %s, got nil", test.result.start)
			}
			if result.End == nil && test.result.end != "" {
				t.Fatalf("Expected end %s, got nil", test.result.end)
			}
			if result.Count != test.result.count {
				t.Fatalf("Expected count %d, got %d", test.result.count, result.Count)
			}
		}
	}
}

func TestDateRangeTimestampQueries(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	imap := mapping.NewIndexMapping()

	// add a date field with a valid format to the default mapping
	// for good measure

	dtParserConfig := map[string]interface{}{
		"type":    flexible.Name,
		"layouts": []interface{}{"2006/01/02 15:04:05"},
	}
	err := imap.AddCustomDateTimeParser("custDT", dtParserConfig)
	if err != nil {
		t.Fatal(err)
	}

	dateField := mapping.NewDateTimeFieldMapping()
	dateField.DateFormat = "custDT"

	unixSecField := mapping.NewDateTimeFieldMapping()
	unixSecField.DateFormat = seconds.Name

	unixMilliSecField := mapping.NewDateTimeFieldMapping()
	unixMilliSecField.DateFormat = milliseconds.Name

	unixMicroSecField := mapping.NewDateTimeFieldMapping()
	unixMicroSecField.DateFormat = microseconds.Name

	unixNanoSecField := mapping.NewDateTimeFieldMapping()
	unixNanoSecField.DateFormat = nanoseconds.Name

	imap.DefaultMapping.AddFieldMappingsAt("date", dateField)
	imap.DefaultMapping.AddFieldMappingsAt("seconds", unixSecField)
	imap.DefaultMapping.AddFieldMappingsAt("milliseconds", unixMilliSecField)
	imap.DefaultMapping.AddFieldMappingsAt("microseconds", unixMicroSecField)
	imap.DefaultMapping.AddFieldMappingsAt("nanoseconds", unixNanoSecField)

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

	documents := map[string]map[string]string{
		"doc1": {
			"date":         "2001/08/20 03:00:10",
			"seconds":      "998276410",
			"milliseconds": "998276410100",
			"microseconds": "998276410100300",
			"nanoseconds":  "998276410100300400",
		},
		"doc2": {
			"date":         "2001/08/20 03:00:20",
			"seconds":      "998276420",
			"milliseconds": "998276410200",
			"microseconds": "998276410100400",
			"nanoseconds":  "998276410100300500",
		},
		"doc3": {
			"date":         "2001/08/20 03:00:30",
			"seconds":      "998276430",
			"milliseconds": "998276410300",
			"microseconds": "998276410100500",
			"nanoseconds":  "998276410100300600",
		},
		"doc4": {
			"date":         "2001/08/20 03:00:40",
			"seconds":      "998276440",
			"milliseconds": "998276410400",
			"microseconds": "998276410100600",
			"nanoseconds":  "998276410100300700",
		},
		"doc5": {
			"date":         "2001/08/20 03:00:50",
			"seconds":      "998276450",
			"milliseconds": "998276410500",
			"microseconds": "998276410100700",
			"nanoseconds":  "998276410100300800",
		},
	}

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	type testStruct struct {
		start        string
		end          string
		field        string
		expectedHits []string
	}

	testQueries := []testStruct{
		{
			start: "2001-08-20T03:00:05",
			end:   "2001-08-20T03:00:25",
			field: "date",
			expectedHits: []string{
				"doc1",
				"doc2",
			},
		},
		{
			start: "2001-08-20T03:00:15",
			end:   "2001-08-20T03:00:35",
			field: "seconds",
			expectedHits: []string{
				"doc2",
				"doc3",
			},
		},
		{
			start: "2001-08-20T03:00:10.150",
			end:   "2001-08-20T03:00:10.450",
			field: "milliseconds",
			expectedHits: []string{
				"doc2",
				"doc3",
				"doc4",
			},
		},
		{
			start: "2001-08-20T03:00:10.100450",
			end:   "2001-08-20T03:00:10.100650",
			field: "microseconds",
			expectedHits: []string{
				"doc3",
				"doc4",
			},
		},
		{
			start: "2001-08-20T03:00:10.100300550",
			end:   "2001-08-20T03:00:10.100300850",
			field: "nanoseconds",
			expectedHits: []string{
				"doc3",
				"doc4",
				"doc5",
			},
		},
	}
	testLayout := "2006-01-02T15:04:05"
	for _, dtq := range testQueries {
		startTime, err := time.Parse(testLayout, dtq.start)
		if err != nil {
			t.Fatal(err)
		}
		endTime, err := time.Parse(testLayout, dtq.end)
		if err != nil {
			t.Fatal(err)
		}
		drq := NewDateRangeQuery(startTime, endTime)
		drq.SetField(dtq.field)

		sr := NewSearchRequest(drq)
		sr.SortBy([]string{dtq.field})
		sr.Fields = []string{"*"}

		res, err := idx.Search(sr)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != len(dtq.expectedHits) {
			t.Fatalf("expected %d hits, got %d", len(dtq.expectedHits), len(res.Hits))
		}
		for i, hit := range res.Hits {
			if hit.ID != dtq.expectedHits[i] {
				t.Fatalf("expected docID %s, got %s", dtq.expectedHits[i], hit.ID)
			}
			if len(hit.Fields) != len(documents[hit.ID]) {
				t.Fatalf("expected hit %s to have %d fields, got %d", hit.ID, len(documents[hit.ID]), len(hit.Fields))
			}
			for k, v := range documents[hit.ID] {
				if hit.Fields[k] != v {
					t.Fatalf("expected field %s to be %s, got %s", k, v, hit.Fields[k])
				}
			}
		}
	}
}

func TestPercentAndIsoStyleDates(t *testing.T) {
	percentName := percent.Name
	isoName := iso.Name

	imap := mapping.NewIndexMapping()
	percentConfig := map[string]interface{}{
		"type": percentName,
		"layouts": []interface{}{
			"%Y/%m/%d %l:%M%p",                // doc 1
			"%d/%m/%Y %H:%M:%S",               // doc 2
			"%Y-%m-%dT%H:%M:%S%z",             // doc 3
			"%d %B %y %l%p %Z",                // doc 4
			"%Y; %b %d (%a) %I:%M:%S.%N%P %z", // doc 5
		},
	}
	isoConfig := map[string]interface{}{
		"type": isoName,
		"layouts": []interface{}{
			"yyyy/MM/dd h:mma",                       // doc 1
			"dd/MM/yyyy HH:mm:ss",                    // doc 2
			"yyyy-MM-dd'T'HH:mm:ssXX",                // doc 3
			"dd MMMM yy ha z",                        // doc 4
			"yyyy; MMM dd (EEE) hh:mm:ss.SSSSSaa xx", // doc 5
		},
	}

	err := imap.AddCustomDateTimeParser("percentDate", percentConfig)
	if err != nil {
		t.Fatal(err)
	}
	err = imap.AddCustomDateTimeParser("isoDate", isoConfig)
	if err != nil {
		t.Fatal(err)
	}

	percentField := mapping.NewDateTimeFieldMapping()
	percentField.DateFormat = "percentDate"

	isoField := mapping.NewDateTimeFieldMapping()
	isoField.DateFormat = "isoDate"

	imap.DefaultMapping.AddFieldMappingsAt("percentDate", percentField)
	imap.DefaultMapping.AddFieldMappingsAt("isoDate", isoField)

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
	}()

	documents := map[string]map[string]interface{}{
		"doc1": {
			"percentDate": "2001/08/20 6:00PM",
			"isoDate":     "2001/08/20 6:00PM",
		},
		"doc2": {
			"percentDate": "20/08/2001 18:05:00",
			"isoDate":     "20/08/2001 18:05:00",
		},
		"doc3": {
			"percentDate": "2001-08-20T18:10:00Z",
			"isoDate":     "2001-08-20T18:10:00Z",
		},
		"doc4": {
			"percentDate": "20 August 01 6PM UTC",
			"isoDate":     "20 August 01 6PM UTC",
		},
		"doc5": {
			"percentDate": "2001; Aug 20 (Mon) 06:15:15.23456pm +0000",
			"isoDate":     "2001; Aug 20 (Mon) 06:15:15.23456pm +0000",
		},
	}

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	type testStruct struct {
		start string
		end   string
		field string
	}

	for _, field := range []string{"percentDate", "isoDate"} {
		testQueries := []testStruct{
			{
				start: "2001/08/20 6:00PM",
				end:   "2001/08/20 6:20PM",
				field: field,
			},
			{
				start: "20/08/2001 18:00:00",
				end:   "20/08/2001 18:20:00",
				field: field,
			},
			{
				start: "2001-08-20T18:00:00Z",
				end:   "2001-08-20T18:20:00Z",
				field: field,
			},
			{
				start: "20 August 01 6PM UTC",
				end:   "20 August 01 7PM UTC",
				field: field,
			},
			{
				start: "2001; Aug 20 (Mon) 06:00:00.00000pm +0000",
				end:   "2001; Aug 20 (Mon) 06:20:20.00000pm +0000",
				field: field,
			},
		}
		includeStart := true
		includeEnd := true
		for _, dtq := range testQueries {
			drq := NewDateRangeInclusiveStringQuery(dtq.start, dtq.end, &includeStart, &includeEnd)
			drq.SetField(dtq.field)
			drq.SetDateTimeParser(field)
			sr := NewSearchRequest(drq)
			res, err := idx.Search(sr)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Hits) != 5 {
				t.Fatalf("expected %d hits, got %d", 5, len(res.Hits))
			}
		}
	}
}

func roundToDecimalPlace(num float64, decimalPlaces int) float64 {
	precision := math.Pow(10, float64(decimalPlaces))
	return math.Round(num*precision) / precision
}

func TestScoreBreakdown(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	imap := mapping.NewIndexMapping()
	textField := mapping.NewTextFieldMapping()
	textField.Analyzer = simple.Name
	imap.DefaultMapping.AddFieldMappingsAt("text", textField)

	documents := map[string]map[string]interface{}{
		"doc1": {
			"text": "lorem ipsum dolor sit amet consectetur adipiscing elit do eiusmod tempor",
		},
		"doc2": {
			"text": "lorem dolor amet adipiscing sed eiusmod",
		},
		"doc3": {
			"text": "ipsum sit consectetur elit do tempor",
		},
		"doc4": {
			"text": "lorem ipsum sit amet adipiscing elit do eiusmod",
		},
	}

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

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	type testResult struct {
		docID          string // doc ID of the hit
		score          float64
		scoreBreakdown map[int]float64
	}
	type testStruct struct {
		query      string
		typ        string
		expectHits []testResult
	}
	testQueries := []testStruct{
		{
			// trigger disjunction heap searcher (>10 searchers)
			// expect score breakdown to have a 0 at BLANK
			query: `{"disjuncts":[{"term":"lorem","field":"text"},{"term":"blank","field":"text"},{"term":"ipsum","field":"text"},{"term":"blank","field":"text"},{"term":"blank","field":"text"},{"term":"dolor","field":"text"},{"term":"sit","field":"text"},{"term":"amet","field":"text"},{"term":"consectetur","field":"text"},{"term":"blank","field":"text"},{"term":"adipiscing","field":"text"},{"term":"blank","field":"text"},{"term":"elit","field":"text"},{"term":"sed","field":"text"},{"term":"do","field":"text"},{"term":"eiusmod","field":"text"},{"term":"tempor","field":"text"},{"term":"blank","field":"text"},{"term":"blank","field":"text"}]}`,
			typ:   "disjunction",
			expectHits: []testResult{
				{
					docID:          "doc1",
					score:          0.3034548543819603,
					scoreBreakdown: map[int]float64{0: 0.040398807605268316, 2: 0.040398807605268316, 5: 0.0669862776967768, 6: 0.040398807605268316, 7: 0.040398807605268316, 8: 0.0669862776967768, 10: 0.040398807605268316, 12: 0.040398807605268316, 14: 0.040398807605268316, 15: 0.040398807605268316, 16: 0.0669862776967768},
				},
				{
					docID:          "doc2",
					score:          0.14725661652397853,
					scoreBreakdown: map[int]float64{0: 0.05470024557900147, 5: 0.09069985124905133, 7: 0.05470024557900147, 10: 0.05470024557900147, 13: 0.15681178542754148, 15: 0.05470024557900147},
				},
				{
					docID:          "doc3",
					score:          0.12637916362550797,
					scoreBreakdown: map[int]float64{2: 0.05470024557900147, 6: 0.05470024557900147, 8: 0.09069985124905133, 12: 0.05470024557900147, 14: 0.05470024557900147, 16: 0.09069985124905133},
				},
				{
					docID:          "doc4",
					score:          0.15956816751152955,
					scoreBreakdown: map[int]float64{0: 0.04737179972998534, 2: 0.04737179972998534, 6: 0.04737179972998534, 7: 0.04737179972998534, 10: 0.04737179972998534, 12: 0.04737179972998534, 14: 0.04737179972998534, 15: 0.04737179972998534},
				},
			},
		},
		{
			// trigger disjunction slice searcher (< 10 searchers)
			// expect BLANK to give a 0 in score breakdown
			query: `{"disjuncts":[{"term":"blank","field":"text"},{"term":"lorem","field":"text"},{"term":"ipsum","field":"text"},{"term":"blank","field":"text"},{"term":"blank","field":"text"},{"term":"dolor","field":"text"},{"term":"sit","field":"text"},{"term":"blank","field":"text"}]}`,
			typ:   "disjunction",
			expectHits: []testResult{
				{
					docID:          "doc1",
					score:          0.1340684440934241,
					scoreBreakdown: map[int]float64{1: 0.05756326446708409, 2: 0.05756326446708409, 5: 0.09544709478559595, 6: 0.05756326446708409},
				},
				{
					docID:          "doc2",
					score:          0.05179425287147191,
					scoreBreakdown: map[int]float64{1: 0.0779410306721006, 5: 0.129235980813787},
				},
				{
					docID:          "doc3",
					score:          0.0389705153360503,
					scoreBreakdown: map[int]float64{2: 0.0779410306721006, 6: 0.0779410306721006},
				},
				{
					docID:          "doc4",
					score:          0.07593627256602972,
					scoreBreakdown: map[int]float64{1: 0.06749890894758198, 2: 0.06749890894758198, 6: 0.06749890894758198},
				},
			},
		},
	}
	for _, dtq := range testQueries {
		var q query.Query
		var rv query.DisjunctionQuery
		err := json.Unmarshal([]byte(dtq.query), &rv)
		if err != nil {
			t.Fatal(err)
		}
		rv.RetrieveScoreBreakdown(true)
		q = &rv
		sr := NewSearchRequest(q)
		sr.SortBy([]string{"_id"})
		sr.Explain = true
		res, err := idx.Search(sr)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != len(dtq.expectHits) {
			t.Fatalf("expected %d hits, got %d", len(dtq.expectHits), len(res.Hits))
		}
		for i, hit := range res.Hits {
			if hit.ID != dtq.expectHits[i].docID {
				t.Fatalf("expected docID %s, got %s", dtq.expectHits[i].docID, hit.ID)
			}
			if len(hit.ScoreBreakdown) != len(dtq.expectHits[i].scoreBreakdown) {
				t.Fatalf("expected %d score breakdown, got %d", len(dtq.expectHits[i].scoreBreakdown), len(hit.ScoreBreakdown))
			}
			for j, score := range hit.ScoreBreakdown {
				actualScore := roundToDecimalPlace(score, 3)
				expectScore := roundToDecimalPlace(dtq.expectHits[i].scoreBreakdown[j], 3)
				if actualScore != expectScore {
					t.Fatalf("expected score breakdown %f, got %f", dtq.expectHits[i].scoreBreakdown[j], score)
				}
			}
		}
	}
}

func TestAutoFuzzy(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	imap := mapping.NewIndexMapping()

	if err := imap.AddCustomAnalyzer("splitter", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{lowercase.Name},
	}); err != nil {
		t.Fatal(err)
	}

	textField := mapping.NewTextFieldMapping()
	textField.Analyzer = "splitter"
	textField.Store = true
	textField.IncludeTermVectors = true
	textField.IncludeInAll = true

	imap.DefaultMapping.Dynamic = false
	imap.DefaultMapping.AddFieldMappingsAt("model", textField)

	documents := map[string]map[string]interface{}{
		"product1": {
			"model": "apple iphone 12",
		},
		"product2": {
			"model": "apple iphone 13",
		},
		"product3": {
			"model": "samsung galaxy s22",
		},
		"product4": {
			"model": "samsung galaxy note",
		},
		"product5": {
			"model": "google pixel 5",
		},
		"product6": {
			"model": "oneplus 9 pro",
		},
		"product7": {
			"model": "xiaomi mi 11",
		},
		"product8": {
			"model": "oppo find x3",
		},
		"product9": {
			"model": "vivo x60 pro",
		},
		"product10": {
			"model": "oneplus 8t pro",
		},
		"product11": {
			"model": "nokia xr20",
		},
		"product12": {
			"model": "poco f1",
		},
		"product13": {
			"model": "asus rog 5",
		},
		"product14": {
			"model": "samsung galaxy a15 5g",
		},
		"product15": {
			"model": "tecno camon 17",
		},
	}
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

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	type testStruct struct {
		query      string
		expectHits []string
	}
	testQueries := []testStruct{
		{
			// match query with fuzziness set to 2
			query: `{
					"match" : "applle iphone 12",
					"fuzziness": 2,
					"field" : "model"
				}`,
			expectHits: []string{"product1", "product2", "product7", "product14", "product15", "product12", "product10", "product3", "product6", "product8"},
		},
		{
			// match query with fuzziness set to "auto"
			query: `{
					"match" : "applle iphone 12",
					"fuzziness": "auto",
					"field" : "model"
				}`,
			expectHits: []string{"product1", "product2"},
		},
		{
			// match query with fuzziness set to 2 with `and` operator
			query: `{
					"match" : "applle iphone 12",
					"fuzziness": 2,
					"field" : "model",
					"operator": "and"
				}`,
			expectHits: []string{"product1", "product2"},
		},
		{
			// match query with fuzziness set to "auto" with `and`` operator
			query: `{
					"match" : "applle iphone 12",
					"fuzziness": "auto",
					"field" : "model",
					"operator": "and"
				}`,
			expectHits: []string{"product1"},
		},
		// match phrase query with fuzziness set to 2
		{
			query: `{
					"match_phrase" : "onplus 9 pro",
					"fuzziness": 2,
					"field" : "model"
				}`,
			expectHits: []string{"product6", "product10"},
		},
		// match phrase query with fuzziness set to "auto"
		{
			query: `{
				"match_phrase" : "onplus 9 pro",
				"fuzziness": "auto",
				"field" : "model"
			}`,
			expectHits: []string{"product6"},
		},
	}

	for _, dtq := range testQueries {
		q, err := query.ParseQuery([]byte(dtq.query))
		if err != nil {
			t.Fatal(err)
		}

		sr := NewSearchRequest(q)
		sr.Highlight = NewHighlightWithStyle(ansi.Name)
		sr.SortBy([]string{"-_score", "_id"})
		sr.Fields = []string{"*"}
		sr.Explain = true

		res, err := idx.Search(sr)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != len(dtq.expectHits) {
			t.Fatalf("expected %d hits, got %d", len(dtq.expectHits), len(res.Hits))
		}
		for i, hit := range res.Hits {
			if hit.ID != dtq.expectHits[i] {
				t.Fatalf("expected docID %s, got %s", dtq.expectHits[i], hit.ID)
			}
		}
	}
}

func TestThesaurusTermReader(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	synonymCollection := "collection1"

	synonymSourceName := "english"

	analyzer := simple.Name

	synonymSourceConfig := map[string]interface{}{
		"collection": synonymCollection,
		"analyzer":   analyzer,
	}

	textField := mapping.NewTextFieldMapping()
	textField.Analyzer = analyzer
	textField.SynonymSource = synonymSourceName

	imap := mapping.NewIndexMapping()
	imap.DefaultMapping.AddFieldMappingsAt("text", textField)
	err := imap.AddSynonymSource(synonymSourceName, synonymSourceConfig)
	if err != nil {
		t.Fatal(err)
	}
	err = imap.Validate()
	if err != nil {
		t.Fatal(err)
	}

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

	documents := map[string]map[string]interface{}{
		"doc1": {
			"text": "quick brown fox eats",
		},
		"doc2": {
			"text": "fast red wolf jumps",
		},
		"doc3": {
			"text": "quick red cat runs",
		},
		"doc4": {
			"text": "speedy brown dog barks",
		},
		"doc5": {
			"text": "fast green rabbit hops",
		},
	}

	batch := idx.NewBatch()
	for docID, doc := range documents {
		err := batch.Index(docID, doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	synonymDocuments := map[string]*SynonymDefinition{
		"synDoc1": {
			Synonyms: []string{"quick", "fast", "speedy"},
		},
		"synDoc2": {
			Input:    []string{"color", "colour"},
			Synonyms: []string{"red", "green", "blue", "yellow", "brown"},
		},
		"synDoc3": {
			Input:    []string{"animal", "creature"},
			Synonyms: []string{"fox", "wolf", "cat", "dog", "rabbit"},
		},
		"synDoc4": {
			Synonyms: []string{"eats", "jumps", "runs", "barks", "hops"},
		},
	}

	for synName, synDef := range synonymDocuments {
		err := batch.IndexSynonym(synName, synonymCollection, synDef)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	sco, err := idx.Advanced()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := sco.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	thesReader, ok := reader.(index.ThesaurusReader)
	if !ok {
		t.Fatal("expected thesaurus reader")
	}

	type testStruct struct {
		queryTerm        string
		expectedSynonyms []string
	}

	testQueries := []testStruct{
		{
			queryTerm:        "quick",
			expectedSynonyms: []string{"fast", "speedy"},
		},
		{
			queryTerm:        "red",
			expectedSynonyms: []string{},
		},
		{
			queryTerm:        "color",
			expectedSynonyms: []string{"red", "green", "blue", "yellow", "brown"},
		},
		{
			queryTerm:        "colour",
			expectedSynonyms: []string{"red", "green", "blue", "yellow", "brown"},
		},
		{
			queryTerm:        "animal",
			expectedSynonyms: []string{"fox", "wolf", "cat", "dog", "rabbit"},
		},
		{
			queryTerm:        "creature",
			expectedSynonyms: []string{"fox", "wolf", "cat", "dog", "rabbit"},
		},
		{
			queryTerm:        "fox",
			expectedSynonyms: []string{},
		},
		{
			queryTerm:        "eats",
			expectedSynonyms: []string{"jumps", "runs", "barks", "hops"},
		},
		{
			queryTerm:        "jumps",
			expectedSynonyms: []string{"eats", "runs", "barks", "hops"},
		},
	}

	for _, test := range testQueries {
		str, err := thesReader.ThesaurusTermReader(context.Background(), synonymSourceName, []byte(test.queryTerm))
		if err != nil {
			t.Fatal(err)
		}
		var gotSynonyms []string
		for {
			synonym, err := str.Next()
			if err != nil {
				t.Fatal(err)
			}
			if synonym == "" {
				break
			}
			gotSynonyms = append(gotSynonyms, string(synonym))
		}
		if len(gotSynonyms) != len(test.expectedSynonyms) {
			t.Fatalf("expected %d synonyms, got %d", len(test.expectedSynonyms), len(gotSynonyms))
		}
		sort.Strings(gotSynonyms)
		sort.Strings(test.expectedSynonyms)
		for i, syn := range gotSynonyms {
			if syn != test.expectedSynonyms[i] {
				t.Fatalf("expected synonym %s, got %s", test.expectedSynonyms[i], syn)
			}
		}
	}
}

func TestSynonymSearchQueries(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	synonymCollection := "collection1"

	synonymSourceName := "english"

	analyzer := en.AnalyzerName

	synonymSourceConfig := map[string]interface{}{
		"collection": synonymCollection,
		"analyzer":   analyzer,
	}

	textField := mapping.NewTextFieldMapping()
	textField.Analyzer = analyzer
	textField.SynonymSource = synonymSourceName

	imap := mapping.NewIndexMapping()
	imap.DefaultMapping.AddFieldMappingsAt("text", textField)
	err := imap.AddSynonymSource(synonymSourceName, synonymSourceConfig)
	if err != nil {
		t.Fatal(err)
	}
	err = imap.Validate()
	if err != nil {
		t.Fatal(err)
	}

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

	documents := map[string]map[string]interface{}{
		"doc1": {
			"text": `The hardworking employee consistently strives to exceed expectations.
					His industrious nature makes him a valuable asset to any team.
					His conscientious attention to detail ensures that projects are completed efficiently and accurately.
					He remains persistent even in the face of challenges.`,
		},
		"doc2": {
			"text": `The tranquil surroundings of the retreat provide a perfect escape from the hustle and bustle of city life.
					Guests enjoy the peaceful atmosphere, which is perfect for relaxation and rejuvenation.
					The calm environment offers the ideal place to meditate and connect with nature.
					Even the most stressed individuals find themselves feeling relaxed and at ease.`,
		},
		"doc3": {
			"text": `The house was burned down, leaving only a charred shell behind.
					The intense heat of the flames caused the walls to warp and the roof to cave in.
					The seared remains of the furniture told the story of the blaze.
					The incinerated remains left little more than ashes to remember what once was.`,
		},
		"doc4": {
			"text": `The faithful dog followed its owner everywhere, always loyal and steadfast.
					It was devoted to protecting its family, and its reliable nature meant it could always be trusted.
					In the face of danger, the dog remained calm, knowing its role was to stay vigilant.
					Its trustworthy companionship provided comfort and security.`,
		},
		"doc5": {
			"text": `The lively market is bustling with activity from morning to night.
					The dynamic energy of the crowd fills the air as vendors sell their wares.
					Shoppers wander from stall to stall, captivated by the vibrant colors and energetic atmosphere.
					This place is alive with movement and life.`,
		},
		"doc6": {
			"text": `In moments of crisis, bravery shines through.
					It takes valor to step forward when others are afraid to act.
					Heroes are defined by their guts and nerve, taking risks to protect others.
					Boldness in the face of danger is what sets them apart.`,
		},
		"doc7": {
			"text": `Innovation is the driving force behind progress in every industry.
					The company fosters an environment of invention, encouraging creativity at every level.
					The focus on novelty and improvement means that ideas are always evolving.
					The development of new solutions is at the core of the company's mission.`,
		},
		"doc8": {
			"text": `The blazing sunset cast a radiant glow over the horizon, painting the sky with hues of red and orange.
					The intense heat of the day gave way to a fiery display of color.
					As the sun set, the glowing light illuminated the landscape, creating a breathtaking scene.
					The fiery sky was a sight to behold.`,
		},
		"doc9": {
			"text": `The fertile soil of the valley makes it perfect for farming.
					The productive land yields abundant crops year after year.
					Farmers rely on the rich, fruitful ground to sustain their livelihoods.
					The area is known for its plentiful harvests, supporting both local communities and export markets.`,
		},
		"doc10": {
			"text": `The arid desert is a vast, dry expanse with little water or vegetation.
					The barren landscape stretches as far as the eye can see, offering little respite from the scorching sun.
					The desolate environment is unforgiving to those who venture too far without preparation.
					The parched earth cracks under the heat, creating a harsh, unyielding terrain.`,
		},
		"doc11": {
			"text": `The fox is known for its cunning and intelligence.
					As a predator, it relies on its sharp instincts to outwit its prey.
					Its vulpine nature makes it both mysterious and fascinating.
					The fox's ability to hunt with precision and stealth is what makes it such a formidable hunter.`,
		},
		"doc12": {
			"text": `The dog is often considered man's best friend due to its loyal nature.
					As a companion, the hound provides both protection and affection.
					The puppy quickly becomes a member of the family, always by your side.
					Its playful energy and unshakable loyalty make it a beloved pet.`,
		},
		"doc13": {
			"text": `He worked tirelessly through the night, always persistent in his efforts.
					His industrious approach to problem-solving kept the project moving forward.
					No matter how difficult the task, he remained focused, always giving his best.
					His dedication paid off when the project was completed ahead of schedule.`,
		},
		"doc14": {
			"text": `The river flowed calmly through the valley, its peaceful current offering a sense of tranquility.
					Fishermen relaxed by the banks, enjoying the calm waters that reflected the sky above.
					The tranquil nature of the river made it a perfect spot for meditation.
					As the day ended, the river's quiet flow brought a sense of peace.`,
		},
		"doc15": {
			"text": `After the fire, all that was left was the charred remains of what once was.
					The seared walls of the house told a tragic story.
					The intensity of the blaze had burned everything in its path, leaving only the smoldering wreckage behind.
					The incinerated objects could not be salvaged, and the damage was beyond repair.`,
		},
		"doc16": {
			"text": `The devoted employee always went above and beyond to complete his tasks.
					His steadfast commitment to the company made him a valuable team member.
					He was reliable, never failing to meet deadlines.
					His trustworthiness earned him the respect of his colleagues, and was considered an
					ingenious expert in his field.`,
		},
		"doc17": {
			"text": `The city is vibrant, full of life and energy.
					The dynamic pace of the streets reflects the diverse culture of its inhabitants.
					People from all walks of life contribute to the energetic atmosphere.
					The city's lively spirit can be felt in every corner, from the bustling markets to the lively festivals.`,
		},
		"doc18": {
			"text": `In a moment of uncertainty, he made a bold decision that would change his life forever.
					It took courage and nerve to take the leap, but his bravery paid off.
					The guts to face the unknown allowed him to achieve something remarkable.
					Being an bright scholar, the skill he demonstrated inspired those around him.`,
		},
		"doc19": {
			"text": `Innovation is often born from necessity, and the lightbulb is a prime example.
					Thomas Edison's invention changed the world, offering a new way to see the night.
					The creativity involved in developing such a groundbreaking product sparked a wave of
					novelty in the scientific community. This improvement in technology continues to shape the modern world.
					He was a clever academic and a smart researcher.`,
		},
		"doc20": {
			"text": `The fiery volcano erupted with a force that shook the earth. Its radiant lava flowed down the sides,
					illuminating the night sky. The intense heat from the eruption could be felt miles away, as the
					glowing lava burned everything in its path. The fiery display was both terrifying and mesmerizing.`,
		},
	}

	synonymDocuments := map[string]*SynonymDefinition{
		"synDoc1": {
			Synonyms: []string{"hardworking", "industrious", "conscientious", "persistent", "focused", "devoted"},
		},
		"synDoc2": {
			Synonyms: []string{"tranquil", "peaceful", "calm", "relaxed", "unruffled"},
		},
		"synDoc3": {
			Synonyms: []string{"burned", "charred", "seared", "incinerated", "singed"},
		},
		"synDoc4": {
			Synonyms: []string{"faithful", "steadfast", "devoted", "reliable", "trustworthy"},
		},
		"synDoc5": {
			Synonyms: []string{"lively", "dynamic", "energetic", "vivid", "vibrating"},
		},
		"synDoc6": {
			Synonyms: []string{"bravery", "valor", "guts", "nerve", "boldness"},
		},
		"synDoc7": {
			Input:    []string{"innovation"},
			Synonyms: []string{"invention", "creativity", "novelty", "improvement", "development"},
		},
		"synDoc8": {
			Input:    []string{"blazing"},
			Synonyms: []string{"intense", "radiant", "burning", "fiery", "glowing"},
		},
		"synDoc9": {
			Input:    []string{"fertile"},
			Synonyms: []string{"productive", "fruitful", "rich", "abundant", "plentiful"},
		},
		"synDoc10": {
			Input:    []string{"arid"},
			Synonyms: []string{"dry", "barren", "desolate", "parched", "unfertile"},
		},
		"synDoc11": {
			Input:    []string{"fox"},
			Synonyms: []string{"vulpine", "canine", "predator", "hunter", "pursuer"},
		},
		"synDoc12": {
			Input:    []string{"dog"},
			Synonyms: []string{"canine", "hound", "puppy", "pup", "companion"},
		},
		"synDoc13": {
			Synonyms: []string{"researcher", "scientist", "scholar", "academic", "expert"},
		},
		"synDoc14": {
			Synonyms: []string{"bright", "clever", "ingenious", "sharp", "astute", "smart"},
		},
	}

	// Combine both maps into a slice of map entries (as they both have similar structure)
	var combinedDocIDs []string
	for id := range synonymDocuments {
		combinedDocIDs = append(combinedDocIDs, id)
	}
	for id := range documents {
		combinedDocIDs = append(combinedDocIDs, id)
	}
	rand.Shuffle(len(combinedDocIDs), func(i, j int) {
		combinedDocIDs[i], combinedDocIDs[j] = combinedDocIDs[j], combinedDocIDs[i]
	})

	// Function to create batches of 5
	createDocBatches := func(docs []string, batchSize int) [][]string {
		var batches [][]string
		for i := 0; i < len(docs); i += batchSize {
			end := i + batchSize
			if end > len(docs) {
				end = len(docs)
			}
			batches = append(batches, docs[i:end])
		}
		return batches
	}
	// Create batches of 5 documents
	batchSize := 5
	docBatches := createDocBatches(combinedDocIDs, batchSize)
	if len(docBatches) == 0 {
		t.Fatal("expected batches")
	}
	totalDocs := 0
	for _, batch := range docBatches {
		totalDocs += len(batch)
	}
	if totalDocs != len(combinedDocIDs) {
		t.Fatalf("expected %d documents, got %d", len(combinedDocIDs), totalDocs)
	}

	var batches []*Batch
	for _, docBatch := range docBatches {
		batch := idx.NewBatch()
		for _, docID := range docBatch {
			if synDef, ok := synonymDocuments[docID]; ok {
				err := batch.IndexSynonym(docID, synonymCollection, synDef)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				err := batch.Index(docID, documents[docID])
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		batches = append(batches, batch)
	}
	for _, batch := range batches {
		err = idx.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

	type testStruct struct {
		query      string
		expectHits []string
	}

	testQueries := []testStruct{
		{
			query: `{
				"match": "hardworking employee",
				"field": "text"
			}`,
			expectHits: []string{"doc1", "doc13", "doc16", "doc4", "doc7"},
		},
		{
			query: `{
				"match": "Hardwork and industrius efforts bring lovely and tranqual moments, with a glazing blow of valour.",
				"field": "text",
				"fuzziness": "auto"
			}`,
			expectHits: []string{
				"doc1", "doc13", "doc14", "doc15", "doc16",
				"doc17", "doc18", "doc2", "doc20", "doc3",
				"doc4", "doc5", "doc6", "doc7", "doc8", "doc9",
			},
		},
		{
			query: `{
				"prefix": "in",
				"field": "text"
			}`,
			expectHits: []string{
				"doc1", "doc11", "doc13", "doc15", "doc16",
				"doc17", "doc18", "doc19", "doc2", "doc20",
				"doc3", "doc4", "doc7", "doc8",
			},
		},
		{
			query: `{
				"prefix": "vivid",
				"field": "text"
			}`,
			expectHits: []string{
				"doc17", "doc5",
			},
		},
		{
			query: `{
				"match_phrase": "smart academic",
				"field": "text"
			}`,
			expectHits: []string{"doc16", "doc18", "doc19"},
		},
		{
			query: `{
				"match_phrase": "smrat acedemic",
				"field": "text",
				"fuzziness": "auto"
			}`,
			expectHits: []string{"doc16", "doc18", "doc19"},
		},
		{
			query: `{
				"wildcard": "br*",
				"field": "text"
			}`,
			expectHits: []string{"doc11", "doc14", "doc16", "doc18", "doc19", "doc6", "doc8"},
		},
	}

	getTotalSynonymSearchStat := func(idx Index) int {
		ir, err := idx.Advanced()
		if err != nil {
			t.Fatal(err)
		}
		stat := ir.StatsMap()["synonym_searches"].(uint64)
		return int(stat)
	}

	runTestQueries := func(idx Index) error {
		for _, dtq := range testQueries {
			q, err := query.ParseQuery([]byte(dtq.query))
			if err != nil {
				return err
			}
			sr := NewSearchRequest(q)
			sr.Highlight = NewHighlightWithStyle(ansi.Name)
			sr.SortBy([]string{"_id"})
			sr.Fields = []string{"*"}
			sr.Size = 30
			sr.Explain = true
			res, err := idx.Search(sr)
			if err != nil {
				return err
			}
			if len(res.Hits) != len(dtq.expectHits) {
				return fmt.Errorf("expected %d hits, got %d", len(dtq.expectHits), len(res.Hits))
			}
			// sort the expected hits to match the order of the search results
			sort.Strings(dtq.expectHits)
			for i, hit := range res.Hits {
				if hit.ID != dtq.expectHits[i] {
					return fmt.Errorf("expected docID %s, got %s", dtq.expectHits[i], hit.ID)
				}
			}
		}
		return nil
	}
	err = runTestQueries(idx)
	if err != nil {
		t.Fatal(err)
	}
	// now verify that the stat for number of synonym enabled queries is correct
	totalSynonymSearchStat := getTotalSynonymSearchStat(idx)
	if totalSynonymSearchStat != len(testQueries) {
		t.Fatalf("expected %d synonym searches, got %d", len(testQueries), totalSynonymSearchStat)
	}

	// test with index alias - with 1 batch per index
	numIndexes := len(batches)
	indexes := make([]Index, numIndexes)
	indexesPath := make([]string, numIndexes)
	for i := 0; i < numIndexes; i++ {
		tmpIndexPath := createTmpIndexPath(t)
		idx, err := New(tmpIndexPath, imap)
		if err != nil {
			t.Fatal(err)
		}
		err = idx.Batch(batches[i])
		if err != nil {
			t.Fatal(err)
		}
		indexes[i] = idx
		indexesPath[i] = tmpIndexPath
	}
	defer func() {
		for i := 0; i < numIndexes; i++ {
			err = indexes[i].Close()
			if err != nil {
				t.Fatal(err)
			}

			cleanupTmpIndexPath(t, indexesPath[i])
		}
	}()
	alias := NewIndexAlias(indexes...)

	if err := alias.SetIndexMapping(imap); err != nil {
		t.Fatal(err)
	}

	err = runTestQueries(alias)
	if err != nil {
		t.Fatal(err)
	}
	// verify the synonym search stat for the alias
	totalSynonymSearchStat = getTotalSynonymSearchStat(indexes[0])
	if totalSynonymSearchStat != len(testQueries) {
		t.Fatalf("expected %d synonym searches, got %d", len(testQueries), totalSynonymSearchStat)
	}
	for i := 1; i < numIndexes; i++ {
		idxStat := getTotalSynonymSearchStat(indexes[i])
		if idxStat != totalSynonymSearchStat {
			t.Fatalf("expected %d synonym searches, got %d", totalSynonymSearchStat, idxStat)
		}
	}
	if totalSynonymSearchStat != len(testQueries) {
		t.Fatalf("expected %d synonym searches, got %d", len(testQueries), totalSynonymSearchStat)
	}
	// test with multi-level alias now with two index per alias
	// and having any extra index being in the final alias
	numAliases := numIndexes / 2
	extraIndex := numIndexes % 2
	aliases := make([]IndexAlias, numAliases)
	for i := 0; i < numAliases; i++ {
		alias := NewIndexAlias(indexes[i*2], indexes[i*2+1])
		aliases[i] = alias
	}
	if extraIndex > 0 {
		aliases[numAliases-1].Add(indexes[numIndexes-1])
	}
	alias = NewIndexAlias()

	if err := alias.SetIndexMapping(imap); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numAliases; i++ {
		alias.Add(aliases[i])
	}
	err = runTestQueries(alias)
	if err != nil {
		t.Fatal(err)
	}
	// verify the synonym searches stat for the alias
	totalSynonymSearchStat = getTotalSynonymSearchStat(indexes[0])
	if totalSynonymSearchStat != 2*len(testQueries) {
		t.Fatalf("expected %d synonym searches, got %d", len(testQueries), totalSynonymSearchStat)
	}
	for i := 1; i < numIndexes; i++ {
		idxStat := getTotalSynonymSearchStat(indexes[i])
		if idxStat != totalSynonymSearchStat {
			t.Fatalf("expected %d synonym searches, got %d", totalSynonymSearchStat, idxStat)
		}
	}
}

func TestGeoDistanceInSort(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	fm := mapping.NewGeoPointFieldMapping()
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping.AddFieldMappingsAt("geo", fm)

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

	qp := []float64{0, 0}

	docs := []struct {
		id       string
		point    []float64
		distance float64
	}{
		{
			id:       "1",
			point:    []float64{1, 1},
			distance: geo.Haversin(1, 1, qp[0], qp[1]) * 1000,
		},
		{
			id:       "2",
			point:    []float64{2, 2},
			distance: geo.Haversin(2, 2, qp[0], qp[1]) * 1000,
		},
		{
			id:       "3",
			point:    []float64{3, 3},
			distance: geo.Haversin(3, 3, qp[0], qp[1]) * 1000,
		},
	}

	for _, doc := range docs {
		if err := idx.Index(doc.id, map[string]interface{}{"geo": doc.point}); err != nil {
			t.Fatal(err)
		}
	}

	q := NewGeoDistanceQuery(qp[0], qp[1], "1000000m")
	q.SetField("geo")
	req := NewSearchRequest(q)
	req.Sort = make(search.SortOrder, 0)
	req.Sort = append(req.Sort, &search.SortGeoDistance{
		Field: "geo",
		Desc:  false,
		Unit:  "m",
		Lon:   qp[0],
		Lat:   qp[1],
	})
	res, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}

	for i, doc := range res.Hits {
		hitDist, err := strconv.ParseFloat(doc.Sort[0], 64)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(hitDist-docs[i].distance) > 1 {
			t.Fatalf("distance error greater than 1 meter, expected distance - %v, got - %v", docs[i].distance, hitDist)
		}
	}
}
