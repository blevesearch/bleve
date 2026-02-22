//  Copyright (c) 2026 Couchbase, Inc.
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

package query

import (
	"github.com/blevesearch/bleve/v2/search/searcher"
)

// Context keys used by CustomFilterQuery/CustomScoreQuery to retrieve
// request-scoped hooks from the embedding application (e.g. cbft).
type customFilterKey string
type customScoreKey string

const (
	CustomFilterContextKey customFilterKey = "custom_filter"
	CustomScoreContextKey  customScoreKey  = "custom_score"
)

// CustomFilterFactory lets the embedding application provide request-scoped
// filter callbacks created from query-provided source/params/fields.
type CustomFilterFactory func(source string, params map[string]interface{}, fields []string) (searcher.FilterFunc, error)

// CustomScoreFactory lets the embedding application provide request-scoped
// score callbacks created from query-provided source/params/fields.
type CustomScoreFactory func(source string, params map[string]interface{}, fields []string) (searcher.ScoreFunc, error)
