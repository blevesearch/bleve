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
	"expvar"
	"time"

	"github.com/couchbaselabs/bleve/search"

	// char filters
	_ "github.com/couchbaselabs/bleve/analysis/char_filters/html_char_filter"
	_ "github.com/couchbaselabs/bleve/analysis/char_filters/regexp_char_filter"
	_ "github.com/couchbaselabs/bleve/analysis/char_filters/zero_width_non_joiner"

	// analyzers
	_ "github.com/couchbaselabs/bleve/analysis/analyzers/keyword_analyzer"
	_ "github.com/couchbaselabs/bleve/analysis/analyzers/simple_analyzer"
	_ "github.com/couchbaselabs/bleve/analysis/analyzers/standard_analyzer"

	// token filters
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/apostrophe_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/edge_ngram_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/elision_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/keyword_marker_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/length_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/lower_case_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/ngram_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/stemmer_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/stop_tokens_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/truncate_token_filter"
	_ "github.com/couchbaselabs/bleve/analysis/token_filters/unicode_normalize"

	// tokenizers
	_ "github.com/couchbaselabs/bleve/analysis/tokenizers/regexp_tokenizer"
	_ "github.com/couchbaselabs/bleve/analysis/tokenizers/single_token"
	_ "github.com/couchbaselabs/bleve/analysis/tokenizers/unicode_word_boundary"
	_ "github.com/couchbaselabs/bleve/analysis/tokenizers/whitespace_tokenizer"

	// date time parsers
	_ "github.com/couchbaselabs/bleve/analysis/datetime_parsers/datetime_optional"
	_ "github.com/couchbaselabs/bleve/analysis/datetime_parsers/flexible_go"

	// languages
	_ "github.com/couchbaselabs/bleve/analysis/language/ar"
	_ "github.com/couchbaselabs/bleve/analysis/language/bg"
	_ "github.com/couchbaselabs/bleve/analysis/language/ca"
	_ "github.com/couchbaselabs/bleve/analysis/language/ckb"
	_ "github.com/couchbaselabs/bleve/analysis/language/cs"
	_ "github.com/couchbaselabs/bleve/analysis/language/da"
	_ "github.com/couchbaselabs/bleve/analysis/language/de"
	_ "github.com/couchbaselabs/bleve/analysis/language/el"
	_ "github.com/couchbaselabs/bleve/analysis/language/en"
	_ "github.com/couchbaselabs/bleve/analysis/language/es"
	_ "github.com/couchbaselabs/bleve/analysis/language/eu"
	_ "github.com/couchbaselabs/bleve/analysis/language/fa"
	_ "github.com/couchbaselabs/bleve/analysis/language/fi"
	_ "github.com/couchbaselabs/bleve/analysis/language/fr"
	_ "github.com/couchbaselabs/bleve/analysis/language/ga"
	_ "github.com/couchbaselabs/bleve/analysis/language/gl"
	_ "github.com/couchbaselabs/bleve/analysis/language/hi"
	_ "github.com/couchbaselabs/bleve/analysis/language/hu"
	_ "github.com/couchbaselabs/bleve/analysis/language/hy"
	_ "github.com/couchbaselabs/bleve/analysis/language/id"
	_ "github.com/couchbaselabs/bleve/analysis/language/it"
	_ "github.com/couchbaselabs/bleve/analysis/language/nl"
	_ "github.com/couchbaselabs/bleve/analysis/language/no"
	_ "github.com/couchbaselabs/bleve/analysis/language/porter"
	_ "github.com/couchbaselabs/bleve/analysis/language/pt"
	_ "github.com/couchbaselabs/bleve/analysis/language/ro"
	_ "github.com/couchbaselabs/bleve/analysis/language/ru"
	_ "github.com/couchbaselabs/bleve/analysis/language/sv"
	_ "github.com/couchbaselabs/bleve/analysis/language/th"
	_ "github.com/couchbaselabs/bleve/analysis/language/tr"

	// kv stores
	_ "github.com/couchbaselabs/bleve/index/store/inmem"
	_ "github.com/couchbaselabs/bleve/index/store/leveldb"
)

var bleveExpVar = expvar.NewMap("bleve")

type HighlightConfig struct {
	Highlighters map[string]search.Highlighter
}

type Configuration struct {
	Highlight           *HighlightConfig
	DefaultHighlighter  *string
	ByteArrayConverters map[string]ByteArrayConverter
	DefaultKVStore      string
}

func NewConfiguration() *Configuration {
	return &Configuration{
		Highlight: &HighlightConfig{
			Highlighters: make(map[string]search.Highlighter),
		},
		ByteArrayConverters: make(map[string]ByteArrayConverter),
	}
}

var Config *Configuration

func init() {
	bootStart := time.Now()

	// build the default configuration
	Config = NewConfiguration()

	// register byte array converters
	Config.ByteArrayConverters["string"] = NewStringByteArrayConverter()
	Config.ByteArrayConverters["json"] = NewJSONByteArrayConverter()
	Config.ByteArrayConverters["ignore"] = NewIgnoreByteArrayConverter()

	// register ansi highlighter
	Config.Highlight.Highlighters["ansi"] = search.NewSimpleHighlighter()

	// register html highlighter
	htmlFormatter := search.NewHTMLFragmentFormatterCustom(`<span class="highlight">`, `</span>`)
	htmlHighlighter := search.NewSimpleHighlighter()
	htmlHighlighter.SetFragmentFormatter(htmlFormatter)
	Config.Highlight.Highlighters["html"] = htmlHighlighter

	// set the default highlighter
	htmlHighlighterName := "html"
	Config.DefaultHighlighter = &htmlHighlighterName

	// default kv store
	Config.DefaultKVStore = "leveldb"

	bootDuration := time.Since(bootStart)
	bleveExpVar.Add("bootDuration", int64(bootDuration))
}
