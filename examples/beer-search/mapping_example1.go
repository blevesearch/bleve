//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build example1

package main

import (
	"github.com/blevesearch/bleve"
)

const textFieldAnalyzer = "en"

func buildIndexMapping() (*bleve.IndexMapping, error) {

	nameMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", textFieldAnalyzer,
			true, true, true, true))

	descMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "enNotTooLong",
			true, true, true, true)).
		AddFieldMapping(
		bleve.NewFieldMapping("descriptionLang", "text", "detect_lang",
			false, true, false, false))

	typeMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "keyword",
			true, true, true, true))

	styleMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "keyword",
			true, true, true, true))

	categoryMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "keyword",
			true, true, true, true))

	beerMapping := bleve.NewDocumentMapping().
		AddSubDocumentMapping("name", nameMapping).
		AddSubDocumentMapping("description", descMapping).
		AddSubDocumentMapping("type", typeMapping).
		AddSubDocumentMapping("style", styleMapping).
		AddSubDocumentMapping("category", categoryMapping)

	breweryMapping := bleve.NewDocumentMapping().
		AddSubDocumentMapping("name", nameMapping).
		AddSubDocumentMapping("description", descMapping)

	indexMapping := bleve.NewIndexMapping().
		AddDocumentMapping("beer", beerMapping).
		AddDocumentMapping("brewery", breweryMapping)

	indexMapping.TypeField = "type"
	indexMapping.DefaultAnalyzer = textFieldAnalyzer

	err := indexMapping.AddCustomTokenFilter("notTooLong",
		map[string]interface{}{
			"type":   "truncate_token",
			"length": 5.0,
		})
	if err != nil {
		return nil, err
	}

	err = indexMapping.AddCustomAnalyzer("enNotTooLong",
		map[string]interface{}{
			"type":      "custom",
			"tokenizer": "unicode",
			"token_filters": []string{
				"notTooLong",
				"possessive_en",
				"to_lower",
				"stop_en",
				"stemmer_en",
			},
		})
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}
