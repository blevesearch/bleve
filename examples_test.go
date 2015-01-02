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
)

var mapping *IndexMapping
var example_index Index

func TestMain(m *testing.M) {
	err := os.RemoveAll("path_to_index")
	if err != nil {
		panic(err)
	}
	toRun := m.Run()
	os.RemoveAll("path_to_index")
	os.Exit(toRun)
}

func ExampleNew() {
	mapping = NewIndexMapping()
	example_index, _ = New("path_to_index", mapping)
	count, _ := example_index.DocCount()
	fmt.Println(count)
	// Output:
	// 0
}

func ExampleIndex_indexing() {
	data := struct{ Name string }{Name: "named one"}
	data2 := struct{ Name string }{Name: "great nameless one"}

	// index some data
	example_index.Index("document id 1", data)
	example_index.Index("document id 2", data2)

	// 1 document has been indexed
	count, _ := example_index.DocCount()
	fmt.Println(count)
	// Output:
	// 2
}

func ExampleNewMatchQuery() {
	// finds documents with fields fully matching the given query text
	query := NewMatchQuery("named one")
	search := NewSearchRequest(query)
	searchResults, _ := example_index.Search(search)
	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 1
}

func ExampleNewMatchAllQuery() {
	// finds all documents in the index
	query := NewMatchAllQuery()
	search := NewSearchRequest(query)
	searchResults, _ := example_index.Search(search)
	fmt.Println(len(searchResults.Hits))
	// Output:
	// 2
}

func ExampleNewMatchNoneQuery() {
	// matches no documents in the index
	query := NewMatchNoneQuery()
	search := NewSearchRequest(query)
	searchResults, _ := example_index.Search(search)
	fmt.Println(len(searchResults.Hits))
	// Output:
	// 0
}

func ExampleNewMatchPhraseQuery() {
	// finds all documents with the given phrase in the index
	query := NewMatchPhraseQuery("nameless one")
	search := NewSearchRequest(query)
	searchResults, _ := example_index.Search(search)
	fmt.Println(searchResults.Hits[0].ID)
	// Output:
	// document id 2
}

func ExampleNewNumericRangeQuery() {
	value1 := float64(11)
	value2 := float64(100)
	data := struct{ priority float64 }{priority: float64(15)}
	data2 := struct{ priority float64 }{priority: float64(10)}

	example_index.Index("document id 3", data)
	example_index.Index("document id 4", data2)

	query := NewNumericRangeQuery(&value1, &value2)
	search := NewSearchRequest(query)
	searchResults, _ := example_index.Search(search)
	fmt.Println(len(searchResults.Hits))
	// Output:
	// 1
}
