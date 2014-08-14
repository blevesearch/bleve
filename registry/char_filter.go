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

func RegisterCharFilter(name string, constructor CharFilterConstructor) {
	_, exists := charFilters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate char filter named '%s'", name))
	}
	charFilters[name] = constructor
}

type CharFilterConstructor func(config map[string]interface{}, cache *Cache) (analysis.CharFilter, error)
type CharFilterRegistry map[string]CharFilterConstructor
type CharFilterCache map[string]analysis.CharFilter

func (c CharFilterCache) CharFilterNamed(name string, cache *Cache) (analysis.CharFilter, error) {
	charFilter, cached := c[name]
	if cached {
		return charFilter, nil
	}
	charFilterConstructor, registered := charFilters[name]
	if !registered {
		return nil, fmt.Errorf("no char filter with name or type '%s' registered", name)
	}
	charFilter, err := charFilterConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building char filter: %v", err)
	}
	c[name] = charFilter
	return charFilter, nil
}

func (c CharFilterCache) DefineCharFilter(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.CharFilter, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("char filter named '%s' already defined", name)
	}
	charFilterConstructor, registered := charFilters[typ]
	if !registered {
		return nil, fmt.Errorf("no char filter type '%s' registered", typ)
	}
	charFilter, err := charFilterConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building char filter: %v", err)
	}
	c[name] = charFilter
	return charFilter, nil
}
