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
	"github.com/couchbaselabs/bleve"
)

func buildIndexMapping() *bleve.IndexMapping {

	nameMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "en",
			true, true, true, true))

	descMapping := bleve.NewDocumentMapping().
		AddFieldMapping(
		bleve.NewFieldMapping(
			"", "text", "en",
			true, true, true, true)).
		AddFieldMapping(
		bleve.NewFieldMapping("descriptionLang", "text", "detect_lang",
			false, true, false, false))

	beerMapping := bleve.NewDocumentMapping().
		AddSubDocumentMapping("name", nameMapping).
		AddSubDocumentMapping("description", descMapping)

	breweryMapping := bleve.NewDocumentMapping().
		AddSubDocumentMapping("name", nameMapping).
		AddSubDocumentMapping("description", descMapping)

	indexMapping := bleve.NewIndexMapping().
		AddDocumentMapping("beer", beerMapping).
		AddDocumentMapping("brewery", breweryMapping).
		SetTypeField("type").
		SetDefaultAnalyzer("en")

	return indexMapping
}
