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
	_ "github.com/blugelabs/bleve/analysis/tokenmap"

	// fragment formatters
	_ "github.com/blugelabs/bleve/search/highlight/format/ansi"
	_ "github.com/blugelabs/bleve/search/highlight/format/html"

	// fragmenters
	_ "github.com/blugelabs/bleve/search/highlight/fragmenter/simple"

	// highlighters
	_ "github.com/blugelabs/bleve/search/highlight/highlighter/ansi"
	_ "github.com/blugelabs/bleve/search/highlight/highlighter/html"
	_ "github.com/blugelabs/bleve/search/highlight/highlighter/simple"

	// char filters
	_ "github.com/blugelabs/bleve/analysis/char/asciifolding"
	_ "github.com/blugelabs/bleve/analysis/char/html"
	_ "github.com/blugelabs/bleve/analysis/char/regexp"
	_ "github.com/blugelabs/bleve/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "github.com/blugelabs/bleve/analysis/analyzer/custom"
	_ "github.com/blugelabs/bleve/analysis/analyzer/keyword"
	_ "github.com/blugelabs/bleve/analysis/analyzer/simple"
	_ "github.com/blugelabs/bleve/analysis/analyzer/standard"
	_ "github.com/blugelabs/bleve/analysis/analyzer/web"

	// token filters
	_ "github.com/blugelabs/bleve/analysis/token/apostrophe"
	_ "github.com/blugelabs/bleve/analysis/token/camelcase"
	_ "github.com/blugelabs/bleve/analysis/token/compound"
	_ "github.com/blugelabs/bleve/analysis/token/edgengram"
	_ "github.com/blugelabs/bleve/analysis/token/elision"
	_ "github.com/blugelabs/bleve/analysis/token/keyword"
	_ "github.com/blugelabs/bleve/analysis/token/length"
	_ "github.com/blugelabs/bleve/analysis/token/lowercase"
	_ "github.com/blugelabs/bleve/analysis/token/ngram"
	_ "github.com/blugelabs/bleve/analysis/token/reverse"
	_ "github.com/blugelabs/bleve/analysis/token/shingle"
	_ "github.com/blugelabs/bleve/analysis/token/stop"
	_ "github.com/blugelabs/bleve/analysis/token/truncate"
	_ "github.com/blugelabs/bleve/analysis/token/unicodenorm"
	_ "github.com/blugelabs/bleve/analysis/token/unique"

	// tokenizers
	_ "github.com/blugelabs/bleve/analysis/tokenizer/exception"
	_ "github.com/blugelabs/bleve/analysis/tokenizer/regexp"
	_ "github.com/blugelabs/bleve/analysis/tokenizer/single"
	_ "github.com/blugelabs/bleve/analysis/tokenizer/unicode"
	_ "github.com/blugelabs/bleve/analysis/tokenizer/web"
	_ "github.com/blugelabs/bleve/analysis/tokenizer/whitespace"

	// date time parsers
	_ "github.com/blugelabs/bleve/analysis/datetime/flexible"
	_ "github.com/blugelabs/bleve/analysis/datetime/optional"

	// languages
	_ "github.com/blugelabs/bleve/analysis/lang/ar"
	_ "github.com/blugelabs/bleve/analysis/lang/bg"
	_ "github.com/blugelabs/bleve/analysis/lang/ca"
	_ "github.com/blugelabs/bleve/analysis/lang/cjk"
	_ "github.com/blugelabs/bleve/analysis/lang/ckb"
	_ "github.com/blugelabs/bleve/analysis/lang/cs"
	_ "github.com/blugelabs/bleve/analysis/lang/da"
	_ "github.com/blugelabs/bleve/analysis/lang/de"
	_ "github.com/blugelabs/bleve/analysis/lang/el"
	_ "github.com/blugelabs/bleve/analysis/lang/en"
	_ "github.com/blugelabs/bleve/analysis/lang/es"
	_ "github.com/blugelabs/bleve/analysis/lang/eu"
	_ "github.com/blugelabs/bleve/analysis/lang/fa"
	_ "github.com/blugelabs/bleve/analysis/lang/fi"
	_ "github.com/blugelabs/bleve/analysis/lang/fr"
	_ "github.com/blugelabs/bleve/analysis/lang/ga"
	_ "github.com/blugelabs/bleve/analysis/lang/gl"
	_ "github.com/blugelabs/bleve/analysis/lang/hi"
	_ "github.com/blugelabs/bleve/analysis/lang/hu"
	_ "github.com/blugelabs/bleve/analysis/lang/hy"
	_ "github.com/blugelabs/bleve/analysis/lang/id"
	_ "github.com/blugelabs/bleve/analysis/lang/in"
	_ "github.com/blugelabs/bleve/analysis/lang/it"
	_ "github.com/blugelabs/bleve/analysis/lang/nl"
	_ "github.com/blugelabs/bleve/analysis/lang/no"
	_ "github.com/blugelabs/bleve/analysis/lang/pt"
	_ "github.com/blugelabs/bleve/analysis/lang/ro"
	_ "github.com/blugelabs/bleve/analysis/lang/ru"
	_ "github.com/blugelabs/bleve/analysis/lang/sv"
	_ "github.com/blugelabs/bleve/analysis/lang/tr"

	// kv stores
	_ "github.com/blugelabs/bleve/index/store/boltdb"
	_ "github.com/blugelabs/bleve/index/store/goleveldb"
	_ "github.com/blugelabs/bleve/index/store/gtreap"
	_ "github.com/blugelabs/bleve/index/store/moss"

	// index types
	_ "github.com/blugelabs/bleve/index/upsidedown"
)
