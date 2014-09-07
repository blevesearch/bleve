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

	"github.com/blevesearch/bleve/search/highlight"
)

func RegisterFragmenter(name string, constructor FragmenterConstructor) {
	_, exists := fragmenters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate fragmenter named '%s'", name))
	}
	fragmenters[name] = constructor
}

type FragmenterConstructor func(config map[string]interface{}, cache *Cache) (highlight.Fragmenter, error)
type FragmenterRegistry map[string]FragmenterConstructor
type FragmenterCache map[string]highlight.Fragmenter

func (c FragmenterCache) FragmenterNamed(name string, cache *Cache) (highlight.Fragmenter, error) {
	fragmenter, cached := c[name]
	if cached {
		return fragmenter, nil
	}
	fragmenterConstructor, registered := fragmenters[name]
	if !registered {
		return nil, fmt.Errorf("no fragmenter with name or type '%s' registered", name)
	}
	fragmenter, err := fragmenterConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building fragmenter: %v", err)
	}
	c[name] = fragmenter
	return fragmenter, nil
}

func (c FragmenterCache) DefineFragmenter(name string, typ string, config map[string]interface{}, cache *Cache) (highlight.Fragmenter, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("fragmenter named '%s' already defined", name)
	}
	fragmenterConstructor, registered := fragmenters[typ]
	if !registered {
		return nil, fmt.Errorf("no fragmenter type '%s' registered", typ)
	}
	fragmenter, err := fragmenterConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building fragmenter: %v", err)
	}
	c[name] = fragmenter
	return fragmenter, nil
}

func FragmenterTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range fragmenters {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
