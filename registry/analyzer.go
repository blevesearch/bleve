//  Copyright (c) 2014 Couchbase, Inc.
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

package registry

import (
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
)

type AnalyzerConstructor func(config map[string]interface{}, cache *Cache) (analysis.Analyzer, error)

// This registry serves as a cache of analyzer constructors.
// On the event of index creation/opening, all the analyzers referenced in the
// index mapping are instantiated using the constructors stored in this registry.
// Each Index maintain a cache of instantiated analyzers.
type AnalyzerRegistry struct {
	m    sync.RWMutex
	cons map[string]AnalyzerConstructor
}

func NewAnalyzerRegistry() *AnalyzerRegistry {
	return &AnalyzerRegistry{
		cons: make(map[string]AnalyzerConstructor),
	}
}

func RegisterAnalyzer(name string, constructor AnalyzerConstructor) error {
	analyzers.m.Lock()
	defer analyzers.m.Unlock()

	_, exists := analyzers.cons[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate analyzer named '%s'", name))
	}

	analyzers.cons[name] = constructor

	return nil
}

// Replace an existing analyzer constructor with a new one
// or register a new analyzer constructor if it doesn't exist
//
// It is the caller's responsibility to ensure that all indexes using the
// existing analyzer are closed and rebuilt after replacing the analyzer.
func ReplaceAnalyzer(name string, constructor AnalyzerConstructor) {
	analyzers.m.Lock()
	analyzers.cons[name] = constructor
	analyzers.m.Unlock()
}

// Remove an analyzer constructor from the registry
//
// It is the caller's responsibility to ensure that all indexes using the
// analyzer are closed and rebuilt after deregistering the analyzer.
func DeregisterAnalyzer(name string) {
	analyzers.m.Lock()
	delete(analyzers.cons, name)
	analyzers.m.Unlock()
}

// -----------------------------------------------------------------------------

type AnalyzerCache struct {
	*ConcurrentCache
}

func NewAnalyzerCache() *AnalyzerCache {
	return &AnalyzerCache{
		NewConcurrentCache(),
	}
}

func AnalyzerBuild(name string, config map[string]interface{}, cache *Cache) (interface{}, error) {
	analyzers.m.RLock()
	defer analyzers.m.RUnlock()

	if cons, registered := analyzers.cons[name]; registered {
		analyzer, err := cons(config, cache)
		if err != nil {
			return nil, fmt.Errorf("error building analyzer: %v", err)
		}
		return analyzer, nil
	}

	return nil, fmt.Errorf("no analyzer with name or type '%s' registered", name)
}

func (c *AnalyzerCache) AnalyzerNamed(name string, cache *Cache) (analysis.Analyzer, error) {
	item, err := c.ItemNamed(name, cache, AnalyzerBuild)
	if err != nil {
		return nil, err
	}
	return item.(analysis.Analyzer), nil
}

func (c *AnalyzerCache) DefineAnalyzer(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.Analyzer, error) {
	item, err := c.DefineItem(name, typ, config, cache, AnalyzerBuild)
	if err != nil {
		if err == ErrAlreadyDefined {
			return nil, fmt.Errorf("analyzer named '%s' already defined", name)
		}
		return nil, err
	}
	return item.(analysis.Analyzer), nil
}

func AnalyzerTypesAndInstances() ([]string, []string) {
	analyzers.m.RLock()
	defer analyzers.m.RUnlock()

	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	var types []string
	var instances []string

	for name, cons := range analyzers.cons {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
