//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"fmt"
	"sort"

	_ "github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	"github.com/blevesearch/bleve/registry"
)

func main() {
	fmt.Printf("Bleve Registry:\n")
	printRegistry()
}

func printRegistry() {
	types, instances := registry.CharFilterTypesAndInstances()
	printType("Char Filter", types, instances)

	types, instances = registry.TokenizerTypesAndInstances()
	printType("Tokenizer", types, instances)

	types, instances = registry.TokenMapTypesAndInstances()
	printType("Token Map", types, instances)

	types, instances = registry.TokenFilterTypesAndInstances()
	printType("Token Filter", types, instances)

	types, instances = registry.AnalyzerTypesAndInstances()
	printType("Analyzer", types, instances)

	types, instances = registry.DateTimeParserTypesAndInstances()
	printType("Date Time Parser", types, instances)

	types, instances = registry.KVStoreTypesAndInstances()
	printType("KV Store", types, instances)

	types, instances = registry.ByteArrayConverterTypesAndInstances()
	printType("ByteArrayConverter", types, instances)

	types, instances = registry.FragmentFormatterTypesAndInstances()
	printType("Fragment Formatter", types, instances)

	types, instances = registry.FragmenterTypesAndInstances()
	printType("Fragmenter", types, instances)

	types, instances = registry.HighlighterTypesAndInstances()
	printType("Highlighter", types, instances)
}

func sortStrings(in []string) []string {
	sortedStrings := make(sort.StringSlice, 0, len(in))
	for _, str := range in {
		sortedStrings = append(sortedStrings, str)
	}
	sortedStrings.Sort()
	return sortedStrings
}

func printType(label string, types, instances []string) {
	sortedTypes := sortStrings(types)
	sortedInstances := sortStrings(instances)
	fmt.Printf(label + " Types:\n")
	for _, name := range sortedTypes {
		fmt.Printf("\t%s\n", name)
	}
	fmt.Println()
	fmt.Printf(label + " Instances:\n")
	for _, name := range sortedInstances {
		fmt.Printf("\t%s\n", name)
	}
	fmt.Println()
}
