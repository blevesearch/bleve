//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package config

import (
	// token maps
	_ "github.com/blevesearch/bleve/analysis/token_map"

	// fragment formatters
	_ "github.com/blevesearch/bleve/search/highlight/fragment_formatters/ansi"
	_ "github.com/blevesearch/bleve/search/highlight/fragment_formatters/html"

	// fragmenters
	_ "github.com/blevesearch/bleve/search/highlight/fragmenters/simple"

	// highlighters
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/ansi"
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/html"
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/simple"

	// char filters
	_ "github.com/blevesearch/bleve/analysis/char_filters/html_char_filter"
	_ "github.com/blevesearch/bleve/analysis/char_filters/regexp_char_filter"
	_ "github.com/blevesearch/bleve/analysis/char_filters/zero_width_non_joiner"

	// analyzers
	_ "github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
	_ "github.com/blevesearch/bleve/analysis/analyzers/keyword_analyzer"
	_ "github.com/blevesearch/bleve/analysis/analyzers/simple_analyzer"
	_ "github.com/blevesearch/bleve/analysis/analyzers/standard_analyzer"
	_ "github.com/blevesearch/bleve/analysis/analyzers/web"

	// token filters
	_ "github.com/blevesearch/bleve/analysis/token_filters/apostrophe_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/compound"
	_ "github.com/blevesearch/bleve/analysis/token_filters/edge_ngram_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/elision_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/keyword_marker_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/length_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/lower_case_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/ngram_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/shingle"
	_ "github.com/blevesearch/bleve/analysis/token_filters/stop_tokens_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/truncate_token_filter"
	_ "github.com/blevesearch/bleve/analysis/token_filters/unicode_normalize"

	// tokenizers
	_ "github.com/blevesearch/bleve/analysis/tokenizers/exception"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/regexp_tokenizer"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/single_token"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/unicode"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/web"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/whitespace_tokenizer"

	// date time parsers
	_ "github.com/blevesearch/bleve/analysis/datetime_parsers/datetime_optional"
	_ "github.com/blevesearch/bleve/analysis/datetime_parsers/flexible_go"

	// languages
	_ "github.com/blevesearch/bleve/analysis/language/ar"
	_ "github.com/blevesearch/bleve/analysis/language/bg"
	_ "github.com/blevesearch/bleve/analysis/language/ca"
	_ "github.com/blevesearch/bleve/analysis/language/cjk"
	_ "github.com/blevesearch/bleve/analysis/language/ckb"
	_ "github.com/blevesearch/bleve/analysis/language/cs"
	_ "github.com/blevesearch/bleve/analysis/language/el"
	_ "github.com/blevesearch/bleve/analysis/language/en"
	_ "github.com/blevesearch/bleve/analysis/language/eu"
	_ "github.com/blevesearch/bleve/analysis/language/fa"
	_ "github.com/blevesearch/bleve/analysis/language/fr"
	_ "github.com/blevesearch/bleve/analysis/language/ga"
	_ "github.com/blevesearch/bleve/analysis/language/gl"
	_ "github.com/blevesearch/bleve/analysis/language/hi"
	_ "github.com/blevesearch/bleve/analysis/language/hy"
	_ "github.com/blevesearch/bleve/analysis/language/id"
	_ "github.com/blevesearch/bleve/analysis/language/in"
	_ "github.com/blevesearch/bleve/analysis/language/it"
	_ "github.com/blevesearch/bleve/analysis/language/pt"

	// kv stores
	_ "github.com/blevesearch/bleve/index/store/boltdb"
	_ "github.com/blevesearch/bleve/index/store/goleveldb"
	_ "github.com/blevesearch/bleve/index/store/gtreap"
	_ "github.com/blevesearch/bleve/index/store/moss"

	// index types
	_ "github.com/blevesearch/bleve/index/smolder"
	_ "github.com/blevesearch/bleve/index/upside_down"
)
