//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package registry

import (
	"fmt"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/search/highlight"
)

var stores = make(KVStoreRegistry, 0)

var byteArrayConverters = make(ByteArrayConverterRegistry, 0)

// highlight
var fragmentFormatters = make(FragmentFormatterRegistry, 0)
var fragmenters = make(FragmenterRegistry, 0)
var highlighters = make(HighlighterRegistry, 0)

// analysis
var charFilters = make(CharFilterRegistry, 0)
var tokenizers = make(TokenizerRegistry, 0)
var tokenMaps = make(TokenMapRegistry, 0)
var tokenFilters = make(TokenFilterRegistry, 0)
var analyzers = make(AnalyzerRegistry, 0)
var dateTimeParsers = make(DateTimeParserRegistry, 0)

type Cache struct {
	CharFilters        CharFilterCache
	Tokenizers         TokenizerCache
	TokenMaps          TokenMapCache
	TokenFilters       TokenFilterCache
	Analyzers          AnalyzerCache
	DateTimeParsers    DateTimeParserCache
	FragmentFormatters FragmentFormatterCache
	Fragmenters        FragmenterCache
	Highlighters       HighlighterCache
}

func NewCache() *Cache {
	return &Cache{
		CharFilters:        make(CharFilterCache, 0),
		Tokenizers:         make(TokenizerCache, 0),
		TokenMaps:          make(TokenMapCache, 0),
		TokenFilters:       make(TokenFilterCache, 0),
		Analyzers:          make(AnalyzerCache, 0),
		DateTimeParsers:    make(DateTimeParserCache, 0),
		FragmentFormatters: make(FragmentFormatterCache, 0),
		Fragmenters:        make(FragmenterCache, 0),
		Highlighters:       make(HighlighterCache, 0),
	}
}

func typeFromConfig(config map[string]interface{}) (string, error) {
	typ, ok := config["type"].(string)
	if ok {
		return typ, nil
	}
	return "", fmt.Errorf("unable to determine type")
}

func (c *Cache) CharFilterNamed(name string) (analysis.CharFilter, error) {
	return c.CharFilters.CharFilterNamed(name, c)
}

func (c *Cache) DefineCharFilter(name string, config map[string]interface{}) (analysis.CharFilter, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.CharFilters.DefineCharFilter(name, typ, config, c)
}

func (c *Cache) TokenizerNamed(name string) (analysis.Tokenizer, error) {
	return c.Tokenizers.TokenizerNamed(name, c)
}

func (c *Cache) DefineTokenizer(name string, config map[string]interface{}) (analysis.Tokenizer, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.Tokenizers.DefineTokenizer(name, typ, config, c)
}

func (c *Cache) TokenMapNamed(name string) (analysis.TokenMap, error) {
	return c.TokenMaps.TokenMapNamed(name, c)
}

func (c *Cache) DefineTokenMap(name string, config map[string]interface{}) (analysis.TokenMap, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.TokenMaps.DefineTokenMap(name, typ, config, c)
}

func (c *Cache) TokenFilterNamed(name string) (analysis.TokenFilter, error) {
	return c.TokenFilters.TokenFilterNamed(name, c)
}

func (c *Cache) DefineTokenFilter(name string, config map[string]interface{}) (analysis.TokenFilter, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.TokenFilters.DefineTokenFilter(name, typ, config, c)
}

func (c *Cache) AnalyzerNamed(name string) (*analysis.Analyzer, error) {
	return c.Analyzers.AnalyzerNamed(name, c)
}

func (c *Cache) DefineAnalyzer(name string, config map[string]interface{}) (*analysis.Analyzer, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.Analyzers.DefineAnalyzer(name, typ, config, c)
}

func (c *Cache) DateTimeParserNamed(name string) (analysis.DateTimeParser, error) {
	return c.DateTimeParsers.DateTimeParserNamed(name, c)
}

func (c *Cache) DefineDateTimeParser(name string, config map[string]interface{}) (analysis.DateTimeParser, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.DateTimeParsers.DefineDateTimeParser(name, typ, config, c)
}

func (c *Cache) FragmentFormatterNamed(name string) (highlight.FragmentFormatter, error) {
	return c.FragmentFormatters.FragmentFormatterNamed(name, c)
}

func (c *Cache) DefineFragmentFormatter(name string, config map[string]interface{}) (highlight.FragmentFormatter, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.FragmentFormatters.DefineFragmentFormatter(name, typ, config, c)
}

func (c *Cache) FragmenterNamed(name string) (highlight.Fragmenter, error) {
	return c.Fragmenters.FragmenterNamed(name, c)
}

func (c *Cache) DefineFragmenter(name string, config map[string]interface{}) (highlight.Fragmenter, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.Fragmenters.DefineFragmenter(name, typ, config, c)
}

func (c *Cache) HighlighterNamed(name string) (highlight.Highlighter, error) {
	return c.Highlighters.HighlighterNamed(name, c)
}

func (c *Cache) DefineHighlighter(name string, config map[string]interface{}) (highlight.Highlighter, error) {
	typ, err := typeFromConfig(config)
	if err != nil {
		return nil, err
	}
	return c.Highlighters.DefineHighlighter(name, typ, config, c)
}
