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

func RegisterHighlighter(name string, constructor HighlighterConstructor) {
	_, exists := highlighters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate highlighter named '%s'", name))
	}
	highlighters[name] = constructor
}

type HighlighterConstructor func(config map[string]interface{}, cache *Cache) (highlight.Highlighter, error)
type HighlighterRegistry map[string]HighlighterConstructor
type HighlighterCache map[string]highlight.Highlighter

func (c HighlighterCache) HighlighterNamed(name string, cache *Cache) (highlight.Highlighter, error) {
	highlighter, cached := c[name]
	if cached {
		return highlighter, nil
	}
	highlighterConstructor, registered := highlighters[name]
	if !registered {
		return nil, fmt.Errorf("no highlighter with name or type '%s' registered", name)
	}
	highlighter, err := highlighterConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building highlighter: %v", err)
	}
	c[name] = highlighter
	return highlighter, nil
}

func (c HighlighterCache) DefineHighlighter(name string, typ string, config map[string]interface{}, cache *Cache) (highlight.Highlighter, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("highlighter named '%s' already defined", name)
	}
	highlighterConstructor, registered := highlighters[typ]
	if !registered {
		return nil, fmt.Errorf("no highlighter type '%s' registered", typ)
	}
	highlighter, err := highlighterConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building highlighter: %v", err)
	}
	c[name] = highlighter
	return highlighter, nil
}

func HighlighterTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range highlighters {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
