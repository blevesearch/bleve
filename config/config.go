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
	_ "bleve/v2/analysis/tokenmap"

	// fragment formatters
	_ "bleve/v2/search/highlight/format/ansi"
	_ "bleve/v2/search/highlight/format/html"

	// fragmenters
	_ "bleve/v2/search/highlight/fragmenter/simple"

	// highlighters
	_ "bleve/v2/search/highlight/highlighter/ansi"
	_ "bleve/v2/search/highlight/highlighter/html"
	_ "bleve/v2/search/highlight/highlighter/simple"

	// char filters
	_ "bleve/v2/analysis/char/asciifolding"
	_ "bleve/v2/analysis/char/html"
	_ "bleve/v2/analysis/char/regexp"
	_ "bleve/v2/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "bleve/v2/analysis/analyzer/custom"
	_ "bleve/v2/analysis/analyzer/keyword"
	_ "bleve/v2/analysis/analyzer/simple"
	_ "bleve/v2/analysis/analyzer/standard"
	_ "bleve/v2/analysis/analyzer/web"

	// token filters
	_ "bleve/v2/analysis/token/apostrophe"
	_ "bleve/v2/analysis/token/camelcase"
	_ "bleve/v2/analysis/token/compound"
	_ "bleve/v2/analysis/token/edgengram"
	_ "bleve/v2/analysis/token/elision"
	_ "bleve/v2/analysis/token/keyword"
	_ "bleve/v2/analysis/token/length"
	_ "bleve/v2/analysis/token/lowercase"
	_ "bleve/v2/analysis/token/ngram"
	_ "bleve/v2/analysis/token/reverse"
	_ "bleve/v2/analysis/token/shingle"
	_ "bleve/v2/analysis/token/stop"
	_ "bleve/v2/analysis/token/truncate"
	_ "bleve/v2/analysis/token/unicodenorm"
	_ "bleve/v2/analysis/token/unique"

	// tokenizers
	_ "bleve/v2/analysis/tokenizer/exception"
	_ "bleve/v2/analysis/tokenizer/regexp"
	_ "bleve/v2/analysis/tokenizer/single"
	_ "bleve/v2/analysis/tokenizer/unicode"
	_ "bleve/v2/analysis/tokenizer/web"
	_ "bleve/v2/analysis/tokenizer/whitespace"

	// date time parsers
	_ "bleve/v2/analysis/datetime/flexible"
	_ "bleve/v2/analysis/datetime/optional"

	// languages
	_ "bleve/v2/analysis/lang/ar"
	_ "bleve/v2/analysis/lang/bg"
	_ "bleve/v2/analysis/lang/ca"
	_ "bleve/v2/analysis/lang/cjk"
	_ "bleve/v2/analysis/lang/ckb"
	_ "bleve/v2/analysis/lang/cs"
	_ "bleve/v2/analysis/lang/da"
	_ "bleve/v2/analysis/lang/de"
	_ "bleve/v2/analysis/lang/el"
	_ "bleve/v2/analysis/lang/en"
	_ "bleve/v2/analysis/lang/es"
	_ "bleve/v2/analysis/lang/eu"
	_ "bleve/v2/analysis/lang/fa"
	_ "bleve/v2/analysis/lang/fi"
	_ "bleve/v2/analysis/lang/fr"
	_ "bleve/v2/analysis/lang/ga"
	_ "bleve/v2/analysis/lang/gl"
	_ "bleve/v2/analysis/lang/hi"
	_ "bleve/v2/analysis/lang/hr"
	_ "bleve/v2/analysis/lang/hu"
	_ "bleve/v2/analysis/lang/hy"
	_ "bleve/v2/analysis/lang/id"
	_ "bleve/v2/analysis/lang/in"
	_ "bleve/v2/analysis/lang/it"
	_ "bleve/v2/analysis/lang/nl"
	_ "bleve/v2/analysis/lang/no"
	_ "bleve/v2/analysis/lang/pt"
	_ "bleve/v2/analysis/lang/ro"
	_ "bleve/v2/analysis/lang/ru"
	_ "bleve/v2/analysis/lang/sv"
	_ "bleve/v2/analysis/lang/tr"

	// kv stores
	_ "bleve/v2/index/upsidedown/store/boltdb"
	_ "bleve/v2/index/upsidedown/store/goleveldb"
	_ "bleve/v2/index/upsidedown/store/gtreap"
	_ "bleve/v2/index/upsidedown/store/moss"

	// index types
	_ "bleve/v2/index/upsidedown"
)
