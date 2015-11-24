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

func RegisterTokenizer(name string, constructor TokenizerConstructor) {
	_, exists := tokenizers[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate tokenizer named '%s'", name))
	}
	tokenizers[name] = constructor
}

type TokenizerConstructor func(config map[string]interface{}, cache *Cache) (analysis.Tokenizer, error)
type TokenizerRegistry map[string]TokenizerConstructor
type TokenizerCache map[string]analysis.Tokenizer

func (c TokenizerCache) TokenizerNamed(name string, cache *Cache) (analysis.Tokenizer, error) {
	tokenizer, cached := c[name]
	if cached {
		return tokenizer, nil
	}
	tokenizerConstructor, registered := tokenizers[name]
	if !registered {
		return nil, fmt.Errorf("no tokenizer with name or type '%s' registered", name)
	}
	tokenizer, err := tokenizerConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building tokenizer '%s': %v", name, err)
	}
	c[name] = tokenizer
	return tokenizer, nil
}

func (c TokenizerCache) DefineTokenizer(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.Tokenizer, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("tokenizer named '%s' already defined", name)
	}
	tokenizerConstructor, registered := tokenizers[typ]
	if !registered {
		return nil, fmt.Errorf("no tokenizer type '%s' registered", typ)
	}
	tokenizer, err := tokenizerConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building tokenizer '%s': %v", name, err)
	}
	c[name] = tokenizer
	return tokenizer, nil
}

func TokenizerTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range tokenizers {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
