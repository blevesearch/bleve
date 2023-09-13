//  Copyright (c) 2015 Couchbase, Inc.
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

package config

import (
	// token maps
	_ "github.com/blevesearch/bleve/v2/analysis/tokenmap"

	// fragment formatters
	_ "github.com/blevesearch/bleve/v2/search/highlight/format/ansi"
	_ "github.com/blevesearch/bleve/v2/search/highlight/format/html"

	// fragmenters
	_ "github.com/blevesearch/bleve/v2/search/highlight/fragmenter/simple"

	// highlighters
	_ "github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"
	_ "github.com/blevesearch/bleve/v2/search/highlight/highlighter/html"
	_ "github.com/blevesearch/bleve/v2/search/highlight/highlighter/simple"

	// char filters
	_ "github.com/blevesearch/bleve/v2/analysis/char/asciifolding"
	_ "github.com/blevesearch/bleve/v2/analysis/char/html"
	_ "github.com/blevesearch/bleve/v2/analysis/char/regexp"
	_ "github.com/blevesearch/bleve/v2/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/web"

	// token filters
	_ "github.com/blevesearch/bleve/v2/analysis/token/apostrophe"
	_ "github.com/blevesearch/bleve/v2/analysis/token/camelcase"
	_ "github.com/blevesearch/bleve/v2/analysis/token/compound"
	_ "github.com/blevesearch/bleve/v2/analysis/token/edgengram"
	_ "github.com/blevesearch/bleve/v2/analysis/token/elision"
	_ "github.com/blevesearch/bleve/v2/analysis/token/keyword"
	_ "github.com/blevesearch/bleve/v2/analysis/token/length"
	_ "github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	_ "github.com/blevesearch/bleve/v2/analysis/token/ngram"
	_ "github.com/blevesearch/bleve/v2/analysis/token/reverse"
	_ "github.com/blevesearch/bleve/v2/analysis/token/shingle"
	_ "github.com/blevesearch/bleve/v2/analysis/token/stop"
	_ "github.com/blevesearch/bleve/v2/analysis/token/truncate"
	_ "github.com/blevesearch/bleve/v2/analysis/token/unicodenorm"
	_ "github.com/blevesearch/bleve/v2/analysis/token/unique"

	// tokenizers
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/exception"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/regexp"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/single"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/web"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"

	// date time parsers
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/flexible"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/iso"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/optional"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/percent"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/sanitized"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/microseconds"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/milliseconds"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/nanoseconds"
	_ "github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/seconds"

	// languages
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ar"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/bg"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ca"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/cjk"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ckb"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/cs"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/da"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/de"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/el"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/en"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/es"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/eu"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/fa"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/fi"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/fr"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ga"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/gl"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/hi"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/hr"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/hu"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/hy"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/id"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/in"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/it"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/nl"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/no"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/pl"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/pt"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ro"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/ru"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/sv"
	_ "github.com/blevesearch/bleve/v2/analysis/lang/tr"

	// kv stores
	_ "github.com/blevesearch/bleve/v2/index/upsidedown/store/boltdb"
	_ "github.com/blevesearch/bleve/v2/index/upsidedown/store/goleveldb"
	_ "github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	_ "github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"

	// index types
	_ "github.com/blevesearch/bleve/v2/index/upsidedown"
)
