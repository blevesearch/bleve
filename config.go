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
	"fmt"
	"regexp"

	"github.com/couchbaselabs/bleve/analysis"

	"github.com/couchbaselabs/bleve/analysis/char_filters/regexp_char_filter"

	"github.com/couchbaselabs/bleve/analysis/tokenizers/regexp_tokenizer"
	"github.com/couchbaselabs/bleve/analysis/tokenizers/single_token"
	"github.com/couchbaselabs/bleve/analysis/tokenizers/unicode_word_boundary"

	"github.com/couchbaselabs/bleve/analysis/token_filters/cld2"
	"github.com/couchbaselabs/bleve/analysis/token_filters/length_filter"
	"github.com/couchbaselabs/bleve/analysis/token_filters/lower_case_filter"
	"github.com/couchbaselabs/bleve/analysis/token_filters/stemmer_filter"
	"github.com/couchbaselabs/bleve/analysis/token_filters/stop_words_filter"

	"github.com/couchbaselabs/bleve/search"
)

type AnalysisConfig struct {
	CharFilters  map[string]analysis.CharFilter
	Tokenizers   map[string]analysis.Tokenizer
	TokenFilters map[string]analysis.TokenFilter
	Analyzers    map[string]*analysis.Analyzer
}

type HighlightConfig struct {
	Highlighters map[string]search.Highlighter
}

type Config struct {
	Analysis           *AnalysisConfig
	DefaultAnalyzer    *string
	Highlight          *HighlightConfig
	DefaultHighlighter *string
}

func (c *Config) BuildNewAnalyzer(charFilterNames []string, tokenizerName string, tokenFilterNames []string) (*analysis.Analyzer, error) {
	rv := analysis.Analyzer{}
	if len(charFilterNames) > 0 {
		rv.CharFilters = make([]analysis.CharFilter, len(charFilterNames))
		for i, charFilterName := range charFilterNames {
			charFilter := c.Analysis.CharFilters[charFilterName]
			if charFilter == nil {
				return nil, fmt.Errorf("no character filter named `%s` registered", charFilterName)
			}
			rv.CharFilters[i] = charFilter
		}
	}
	rv.Tokenizer = c.Analysis.Tokenizers[tokenizerName]
	if rv.Tokenizer == nil {
		return nil, fmt.Errorf("no tokenizer named `%s` registered", tokenizerName)
	}
	if len(tokenFilterNames) > 0 {
		rv.TokenFilters = make([]analysis.TokenFilter, len(tokenFilterNames))
		for i, tokenFilterName := range tokenFilterNames {
			tokenFilter := c.Analysis.TokenFilters[tokenFilterName]
			if tokenFilter == nil {
				return nil, fmt.Errorf("no token filter named `%s` registered", tokenFilterName)
			}
			rv.TokenFilters[i] = tokenFilter
		}
	}
	return &rv, nil
}

func (c *Config) MustBuildNewAnalyzer(charFilterNames []string, tokenizerName string, tokenFilterNames []string) *analysis.Analyzer {
	analyzer, err := c.BuildNewAnalyzer(charFilterNames, tokenizerName, tokenFilterNames)
	if err != nil {
		panic(err)
	}
	return analyzer
}

func NewConfig() *Config {
	return &Config{
		Analysis: &AnalysisConfig{
			CharFilters:  make(map[string]analysis.CharFilter),
			Tokenizers:   make(map[string]analysis.Tokenizer),
			TokenFilters: make(map[string]analysis.TokenFilter),
			Analyzers:    make(map[string]*analysis.Analyzer),
		},
		Highlight: &HighlightConfig{
			Highlighters: make(map[string]search.Highlighter),
		},
	}
}

var config *Config

func init() {

	// build the default configuration
	config = NewConfig()

	// register char filters
	htmlCharFilterRegexp := regexp.MustCompile(`</?[!\w]+((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[^'">\s]+))?)+\s*|\s*)/?>`)
	htmlCharFilter := regexp_char_filter.NewRegexpCharFilter(htmlCharFilterRegexp, []byte{' '})
	config.Analysis.CharFilters["html"] = htmlCharFilter

	// register tokenizers
	whitespaceTokenizerRegexp := regexp.MustCompile(`\w+`)
	config.Analysis.Tokenizers["single"] = single_token.NewSingleTokenTokenizer()
	config.Analysis.Tokenizers["unicode"] = unicode_word_boundary.NewUnicodeWordBoundaryTokenizer()
	config.Analysis.Tokenizers["unicode_th"] = unicode_word_boundary.NewUnicodeWordBoundaryCustomLocaleTokenizer("th_TH")
	config.Analysis.Tokenizers["whitespace"] = regexp_tokenizer.NewRegexpTokenizer(whitespaceTokenizerRegexp)

	// register token filters
	config.Analysis.TokenFilters["detect_lang"] = cld2.NewCld2Filter()
	config.Analysis.TokenFilters["short"] = length_filter.NewLengthFilter(3, -1)
	config.Analysis.TokenFilters["long"] = length_filter.NewLengthFilter(-1, 255)
	config.Analysis.TokenFilters["to_lower"] = lower_case_filter.NewLowerCaseFilter()
	config.Analysis.TokenFilters["stemmer_da"] = stemmer_filter.MustNewStemmerFilter("danish")
	config.Analysis.TokenFilters["stemmer_nl"] = stemmer_filter.MustNewStemmerFilter("dutch")
	config.Analysis.TokenFilters["stemmer_en"] = stemmer_filter.MustNewStemmerFilter("english")
	config.Analysis.TokenFilters["stemmer_fi"] = stemmer_filter.MustNewStemmerFilter("finnish")
	config.Analysis.TokenFilters["stemmer_fr"] = stemmer_filter.MustNewStemmerFilter("french")
	config.Analysis.TokenFilters["stemmer_de"] = stemmer_filter.MustNewStemmerFilter("german")
	config.Analysis.TokenFilters["stemmer_hu"] = stemmer_filter.MustNewStemmerFilter("hungarian")
	config.Analysis.TokenFilters["stemmer_it"] = stemmer_filter.MustNewStemmerFilter("italian")
	config.Analysis.TokenFilters["stemmer_no"] = stemmer_filter.MustNewStemmerFilter("norwegian")
	config.Analysis.TokenFilters["stemmer_porter"] = stemmer_filter.MustNewStemmerFilter("porter")
	config.Analysis.TokenFilters["stemmer_pt"] = stemmer_filter.MustNewStemmerFilter("portuguese")
	config.Analysis.TokenFilters["stemmer_ro"] = stemmer_filter.MustNewStemmerFilter("romanian")
	config.Analysis.TokenFilters["stemmer_ru"] = stemmer_filter.MustNewStemmerFilter("russian")
	config.Analysis.TokenFilters["stemmer_es"] = stemmer_filter.MustNewStemmerFilter("spanish")
	config.Analysis.TokenFilters["stemmer_sv"] = stemmer_filter.MustNewStemmerFilter("swedish")
	config.Analysis.TokenFilters["stemmer_tr"] = stemmer_filter.MustNewStemmerFilter("turkish")
	config.Analysis.TokenFilters["stop_token"] = stop_words_filter.NewStopWordsFilter()

	// register analyzers
	keywordAnalyzer := config.MustBuildNewAnalyzer([]string{}, "single", []string{})
	config.Analysis.Analyzers["keyword"] = keywordAnalyzer
	simpleAnalyzer := config.MustBuildNewAnalyzer([]string{}, "whitespace", []string{"to_lower"})
	config.Analysis.Analyzers["simple"] = simpleAnalyzer
	standardAnalyzer := config.MustBuildNewAnalyzer([]string{}, "whitespace", []string{"to_lower", "stop_token"})
	config.Analysis.Analyzers["standard"] = standardAnalyzer
	englishAnalyzer := config.MustBuildNewAnalyzer([]string{}, "unicode", []string{"to_lower", "stemmer_en", "stop_token"})
	config.Analysis.Analyzers["english"] = englishAnalyzer
	detectLangAnalyzer := config.MustBuildNewAnalyzer([]string{}, "single", []string{"to_lower", "detect_lang"})
	config.Analysis.Analyzers["detect_lang"] = detectLangAnalyzer

	// register ansi highlighter
	config.Highlight.Highlighters["ansi"] = search.NewSimpleHighlighter()

	// register html highlighter
	htmlFormatter := search.NewHTMLFragmentFormatterCustom(`<span class="highlight">`, `</span>`)
	htmlHighlighter := search.NewSimpleHighlighter()
	htmlHighlighter.SetFragmentFormatter(htmlFormatter)
	config.Highlight.Highlighters["html"] = htmlHighlighter

	// set the default analyzer
	simpleAnalyzerName := "simple"
	config.DefaultAnalyzer = &simpleAnalyzerName

	// set the default highlighter
	htmlHighlighterName := "html"
	config.DefaultHighlighter = &htmlHighlighterName

}
