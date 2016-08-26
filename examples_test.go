//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/highlight/highlighters/ansi"
)

var mapping *IndexMapping
var example_index Index
var err error

func TestMain(m *testing.M) {
	err = os.RemoveAll("path_to_index")
	if err != nil {
		panic(err)
	}
	toRun := m.Run()
	if example_index != nil {
		err = example_index.Close()
		if err != nil {
			panic(err)
		}
	}
	err = os.RemoveAll("path_to_index")
	if err != nil {
		panic(err)
	}
	os.Exit(toRun)
}

func ExampleNew() {
	mapping = NewIndexMapping()
	example_index, err = New("path_to_index", mapping)
	if err != nil {
		panic(err)
	}
	count, err := example_index.DocCount()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output:
	// 0
}

func ExampleIndex_indexing() {
	data := struct {
		Name    string
		Created time.Time
		Age     int
	}{Name: "named one", Created: time.Now(), Age: 50}
	data2 := struct {
		Name    string
		Created time.Time
		Age     int
	}{Name: "great nameless one", Created: time.Now(), Age: 25}

	// index some data
	err = example_index.Index("document id 1", data)
	if err != nil {
		panic(err)
	}
	err = example_index.Index("document id 2", data2)
	if err != nil {
		panic(err)
	}

	// 2 documents have been indexed
	count, err := example_index.DocCount()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output:
	// 2
}

// Examples for query related functions

func ExampleNewMatchQuery() {
	// finds documents with fields fully matching the given query text
	query := NewMatchQuery("named one")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 1
}

func ExampleNewMatchAllQuery() {
	// finds all documents in the index
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(searchResults.Hits))
	// Output:
	// 2
}

func ExampleNewMatchNoneQuery() {
	// matches no documents in the index
	query := NewMatchNoneQuery()
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(searchResults.Hits))
	// Output:
	// 0
}

func ExampleNewMatchPhraseQuery() {
	// finds all documents with the given phrase in the index
	query := NewMatchPhraseQuery("nameless one")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewNumericRangeQuery() {
	value1 := float64(11)
	value2 := float64(100)
	data := struct{ Priority float64 }{Priority: float64(15)}
	data2 := struct{ Priority float64 }{Priority: float64(10)}

	err = example_index.Index("document id 3", data)
	if err != nil {
		panic(err)
	}
	err = example_index.Index("document id 4", data2)
	if err != nil {
		panic(err)
	}

	query := NewNumericRangeQuery(&value1, &value2)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 3
}

func ExampleNewNumericRangeInclusiveQuery() {
	value1 := float64(10)
	value2 := float64(100)
	v1incl := false
	v2incl := false

	query := NewNumericRangeInclusiveQuery(&value1, &value2, &v1incl, &v2incl)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 3
}

func ExampleNewPhraseQuery() {
	// finds all documents with the given phrases in the given field in the index
	query := NewPhraseQuery([]string{"nameless", "one"}, "Name")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewPrefixQuery() {
	// finds all documents with terms having the given prefix in the index
	query := NewPrefixQuery("name")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(searchResults.Hits))
	// Output:
	// 2
}

func ExampleNewQueryStringQuery() {
	query := NewQueryStringQuery("+one -great")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 1
}

func ExampleNewTermQuery() {
	query := NewTermQuery("great")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewFacetRequest() {
	facet := NewFacetRequest("Name", 1)
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchRequest.AddFacet("facet name", facet)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	// total number of terms
	fmt.Println(searchResults.Facets["facet name"].Total)
	// numer of docs with no value for this field
	fmt.Println(searchResults.Facets["facet name"].Missing)
	// term with highest occurrences in field name
	fmt.Println(searchResults.Facets["facet name"].Terms[0].Term)
	// Output:
	// 5
	// 2
	// one
}

func ExampleFacetRequest_AddDateTimeRange() {
	facet := NewFacetRequest("Created", 1)
	facet.AddDateTimeRange("range name", time.Unix(0, 0), time.Now())
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchRequest.AddFacet("facet name", facet)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	// dates in field Created since starting of unix time till now
	fmt.Println(searchResults.Facets["facet name"].DateRanges[0].Count)
	// Output:
	// 2
}

func ExampleFacetRequest_AddNumericRange() {
	value1 := float64(11)

	facet := NewFacetRequest("Priority", 1)
	facet.AddNumericRange("range name", &value1, nil)
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchRequest.AddFacet("facet name", facet)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	// number documents with field Priority in the given range
	fmt.Println(searchResults.Facets["facet name"].NumericRanges[0].Count)
	// Output:
	// 1
}

func ExampleNewHighlight() {
	query := NewMatchQuery("nameless")
	searchRequest := NewSearchRequest(query)
	searchRequest.Highlight = NewHighlight()
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].Fragments["Name"][0])
	// Output:
	// great <mark>nameless</mark> one
}

func ExampleNewHighlightWithStyle() {
	query := NewMatchQuery("nameless")
	searchRequest := NewSearchRequest(query)
	searchRequest.Highlight = NewHighlightWithStyle(ansi.Name)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].Fragments["Name"][0])
	// Output:
	// great [43mnameless[0m one
}

func ExampleSearchRequest_AddFacet() {
	facet := NewFacetRequest("Name", 1)
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchRequest.AddFacet("facet name", facet)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	// total number of terms
	fmt.Println(searchResults.Facets["facet name"].Total)
	// numer of docs with no value for this field
	fmt.Println(searchResults.Facets["facet name"].Missing)
	// term with highest occurrences in field name
	fmt.Println(searchResults.Facets["facet name"].Terms[0].Term)
	// Output:
	// 5
	// 2
	// one
}

func ExampleNewSearchRequest() {
	// finds documents with fields fully matching the given query text
	query := NewMatchQuery("named one")
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 1
}

func ExampleNewBooleanQuery() {
	must := make([]Query, 1)
	mustNot := make([]Query, 1)
	must[0] = NewMatchQuery("one")
	mustNot[0] = NewMatchQuery("great")
	query := NewBooleanQuery(must, nil, mustNot)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 1
}

func ExampleNewBooleanQueryMinShould() {
	should := make([]Query, 2)
	should[0] = NewMatchQuery("great")
	should[1] = NewMatchQuery("one")
	query := NewBooleanQueryMinShould(nil, should, nil, float64(2))
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewConjunctionQuery() {
	conjuncts := make([]Query, 2)
	conjuncts[0] = NewMatchQuery("great")
	conjuncts[1] = NewMatchQuery("one")
	query := NewConjunctionQuery(conjuncts)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewMatchQueryOperator() {
	query := NewMatchQueryOperator("great one", MatchQueryOperatorAnd)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewDisjunctionQuery() {
	disjuncts := make([]Query, 2)
	disjuncts[0] = NewMatchQuery("great")
	disjuncts[1] = NewMatchQuery("named")
	query := NewDisjunctionQuery(disjuncts)
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(searchResults.Hits))
	// Output:
	// 2
}

func ExampleNewDisjunctionQueryMin() {
	disjuncts := make([]Query, 2)
	disjuncts[0] = NewMatchQuery("great")
	disjuncts[1] = NewMatchQuery("named")
	query := NewDisjunctionQueryMin(disjuncts, float64(2))
	searchRequest := NewSearchRequest(query)
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(searchResults.Hits))
	// Output:
	// 0
}

// Examples for Mapping related functions

func ExampleDocumentMapping_AddSubDocumentMapping() {
	// adds a document mapping for a property in a document
	// useful for mapping nested documents
	documentMapping := NewDocumentMapping()
	subDocumentMapping := NewDocumentMapping()
	documentMapping.AddSubDocumentMapping("Property", subDocumentMapping)

	fmt.Println(len(documentMapping.Properties))
	// Output:
	// 1
}

func ExampleDocumentMapping_AddFieldMapping() {
	// you can only add field mapping to those properties which already have a document mapping
	documentMapping := NewDocumentMapping()
	subDocumentMapping := NewDocumentMapping()
	documentMapping.AddSubDocumentMapping("Property", subDocumentMapping)

	fieldMapping := NewTextFieldMapping()
	fieldMapping.Analyzer = "en"
	subDocumentMapping.AddFieldMapping(fieldMapping)

	fmt.Println(len(documentMapping.Properties["Property"].Fields))
	// Output:
	// 1
}

func ExampleDocumentMapping_AddFieldMappingsAt() {
	// you can only add field mapping to those properties which already have a document mapping
	documentMapping := NewDocumentMapping()
	subDocumentMapping := NewDocumentMapping()
	documentMapping.AddSubDocumentMapping("NestedProperty", subDocumentMapping)

	fieldMapping := NewTextFieldMapping()
	fieldMapping.Analyzer = "en"
	documentMapping.AddFieldMappingsAt("NestedProperty", fieldMapping)

	fmt.Println(len(documentMapping.Properties["NestedProperty"].Fields))
	// Output:
	// 1
}

func ExampleSearchRequest_SortBy() {
	// find docs containing "one", order by Age instead of score
	query := NewMatchQuery("one")
	searchRequest := NewSearchRequest(query)
	searchRequest.SortBy([]string{"Age"})
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	fmt.Println(searchResults.Hits[1].ID)
	// Output:
	// document id 2
	// document id 1
}

func ExampleSearchRequest_SortByCustom() {
	// find all docs, order by Age, with docs missing Age field first
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)
	searchRequest.SortByCustom(search.SortOrder{
		&search.SortField{
			Field:   "Age",
			Missing: search.SortFieldMissingFirst,
		},
	})
	searchResults, err := example_index.Search(searchRequest)
	if err != nil {
		panic(err)
	}

	fmt.Println(searchResults.Hits[0].ID)
	fmt.Println(searchResults.Hits[1].ID)
	fmt.Println(searchResults.Hits[2].ID)
	fmt.Println(searchResults.Hits[3].ID)
	// Output:
	// document id 3
	// document id 4
	// document id 2
	// document id 1
}
