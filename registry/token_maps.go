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

func RegisterTokenMap(name string, constructor TokenMapConstructor) {
	_, exists := tokenMaps[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate token map named '%s'", name))
	}
	tokenMaps[name] = constructor
}

type TokenMapConstructor func(config map[string]interface{}, cache *Cache) (analysis.TokenMap, error)
type TokenMapRegistry map[string]TokenMapConstructor
type TokenMapCache map[string]analysis.TokenMap

func (c TokenMapCache) TokenMapNamed(name string, cache *Cache) (analysis.TokenMap, error) {
	tokenMap, cached := c[name]
	if cached {
		return tokenMap, nil
	}
	tokenMapConstructor, registered := tokenMaps[name]
	if !registered {
		return nil, fmt.Errorf("no token map with name or type '%s' registered", name)
	}
	tokenMap, err := tokenMapConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building token map: %v", err)
	}
	c[name] = tokenMap
	return tokenMap, nil
}

func (c TokenMapCache) DefineTokenMap(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.TokenMap, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("token map named '%s' already defined", name)
	}
	tokenMapConstructor, registered := tokenMaps[typ]
	if !registered {
		return nil, fmt.Errorf("no token map type '%s' registered", typ)
	}
	tokenMap, err := tokenMapConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building token map: %v", err)
	}
	c[name] = tokenMap
	return tokenMap, nil
}
