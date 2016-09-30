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
	_ "github.com/blevesearch/bleve/analysis/tokenmap"

	// fragment formatters
	_ "github.com/blevesearch/bleve/search/highlight/format/ansi"
	_ "github.com/blevesearch/bleve/search/highlight/format/html"

	// fragmenters
	_ "github.com/blevesearch/bleve/search/highlight/fragmenters/simple"

	// highlighters
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/ansi"
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/html"
	_ "github.com/blevesearch/bleve/search/highlight/highlighters/simple"

	// char filters
	_ "github.com/blevesearch/bleve/analysis/char/html"
	_ "github.com/blevesearch/bleve/analysis/char/regexp"
	_ "github.com/blevesearch/bleve/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "github.com/blevesearch/bleve/analysis/analyzers/custom"
	_ "github.com/blevesearch/bleve/analysis/analyzers/keyword"
	_ "github.com/blevesearch/bleve/analysis/analyzers/simple"
	_ "github.com/blevesearch/bleve/analysis/analyzers/standard"
	_ "github.com/blevesearch/bleve/analysis/analyzers/web"

	// token filters
	_ "github.com/blevesearch/bleve/analysis/tokens/apostrophe"
	_ "github.com/blevesearch/bleve/analysis/tokens/compound"
	_ "github.com/blevesearch/bleve/analysis/tokens/edgengram"
	_ "github.com/blevesearch/bleve/analysis/tokens/elision"
	_ "github.com/blevesearch/bleve/analysis/tokens/keyword"
	_ "github.com/blevesearch/bleve/analysis/tokens/length"
	_ "github.com/blevesearch/bleve/analysis/tokens/lowercase"
	_ "github.com/blevesearch/bleve/analysis/tokens/ngram"
	_ "github.com/blevesearch/bleve/analysis/tokens/shingle"
	_ "github.com/blevesearch/bleve/analysis/tokens/stop"
	_ "github.com/blevesearch/bleve/analysis/tokens/truncate"
	_ "github.com/blevesearch/bleve/analysis/tokens/unicodenorm"

	// tokenizers
	_ "github.com/blevesearch/bleve/analysis/tokenizers/exception"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/regexp"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/single"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/unicode"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/web"
	_ "github.com/blevesearch/bleve/analysis/tokenizers/whitespace"

	// date time parsers
	_ "github.com/blevesearch/bleve/analysis/datetime/flexible"
	_ "github.com/blevesearch/bleve/analysis/datetime/optional"

	// languages
	_ "github.com/blevesearch/bleve/analysis/lang/ar"
	_ "github.com/blevesearch/bleve/analysis/lang/bg"
	_ "github.com/blevesearch/bleve/analysis/lang/ca"
	_ "github.com/blevesearch/bleve/analysis/lang/cjk"
	_ "github.com/blevesearch/bleve/analysis/lang/ckb"
	_ "github.com/blevesearch/bleve/analysis/lang/cs"
	_ "github.com/blevesearch/bleve/analysis/lang/el"
	_ "github.com/blevesearch/bleve/analysis/lang/en"
	_ "github.com/blevesearch/bleve/analysis/lang/eu"
	_ "github.com/blevesearch/bleve/analysis/lang/fa"
	_ "github.com/blevesearch/bleve/analysis/lang/fr"
	_ "github.com/blevesearch/bleve/analysis/lang/ga"
	_ "github.com/blevesearch/bleve/analysis/lang/gl"
	_ "github.com/blevesearch/bleve/analysis/lang/hi"
	_ "github.com/blevesearch/bleve/analysis/lang/hy"
	_ "github.com/blevesearch/bleve/analysis/lang/id"
	_ "github.com/blevesearch/bleve/analysis/lang/in"
	_ "github.com/blevesearch/bleve/analysis/lang/it"
	_ "github.com/blevesearch/bleve/analysis/lang/pt"

	// kv stores
	_ "github.com/blevesearch/bleve/index/store/boltdb"
	_ "github.com/blevesearch/bleve/index/store/goleveldb"
	_ "github.com/blevesearch/bleve/index/store/gtreap"
	_ "github.com/blevesearch/bleve/index/store/moss"

	// index types
	_ "github.com/blevesearch/bleve/index/smolder"
	_ "github.com/blevesearch/bleve/index/upsidedown"
)
