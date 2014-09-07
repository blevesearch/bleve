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
)

func RegisterTokenFilter(name string, constructor TokenFilterConstructor) {
	_, exists := tokenFilters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate token filter named '%s'", name))
	}
	tokenFilters[name] = constructor
}

type TokenFilterConstructor func(config map[string]interface{}, cache *Cache) (analysis.TokenFilter, error)
type TokenFilterRegistry map[string]TokenFilterConstructor
type TokenFilterCache map[string]analysis.TokenFilter

func (c TokenFilterCache) TokenFilterNamed(name string, cache *Cache) (analysis.TokenFilter, error) {
	tokenFilter, cached := c[name]
	if cached {
		return tokenFilter, nil
	}
	tokenFilterConstructor, registered := tokenFilters[name]
	if !registered {
		return nil, fmt.Errorf("no token filter with name or type '%s' registered", name)
	}
	tokenFilter, err := tokenFilterConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building token filter: %v", err)
	}
	c[name] = tokenFilter
	return tokenFilter, nil
}

func (c TokenFilterCache) DefineTokenFilter(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.TokenFilter, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("token filter named '%s' already defined", name)
	}
	tokenFilterConstructor, registered := tokenFilters[typ]
	if !registered {
		return nil, fmt.Errorf("no token filter type '%s' registered", typ)
	}
	tokenFilter, err := tokenFilterConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building token filter: %v", err)
	}
	c[name] = tokenFilter
	return tokenFilter, nil
}

func TokenFilterTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range tokenFilters {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
