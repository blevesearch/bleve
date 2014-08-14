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

	"github.com/couchbaselabs/bleve/analysis"
)

func RegisterAnalyzer(name string, constructor AnalyzerConstructor) {
	_, exists := analyzers[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate analyzer named '%s'", name))
	}
	analyzers[name] = constructor
}

type AnalyzerConstructor func(config map[string]interface{}, cache *Cache) (*analysis.Analyzer, error)
type AnalyzerRegistry map[string]AnalyzerConstructor
type AnalyzerCache map[string]*analysis.Analyzer

func (c AnalyzerCache) AnalyzerNamed(name string, cache *Cache) (*analysis.Analyzer, error) {
	analyzer, cached := c[name]
	if cached {
		return analyzer, nil
	}
	analyzerConstructor, registered := analyzers[name]
	if !registered {
		return nil, fmt.Errorf("no analyzer with name or type '%s' registered", name)
	}
	analyzer, err := analyzerConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building analyzer: %v", err)
	}
	c[name] = analyzer
	return analyzer, nil
}

func (c AnalyzerCache) DefineAnalyzer(name string, typ string, config map[string]interface{}, cache *Cache) (*analysis.Analyzer, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("analyzer named '%s' already defined", name)
	}
	analyzerConstructor, registered := analyzers[typ]
	if !registered {
		return nil, fmt.Errorf("no analyzer type '%s' registered", typ)
	}
	analyzer, err := analyzerConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building analyzer: %v", err)
	}
	c[name] = analyzer
	return analyzer, nil
}
