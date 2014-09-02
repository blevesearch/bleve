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

func RegisterFragmentFormatter(name string, constructor FragmentFormatterConstructor) {
	_, exists := fragmentFormatters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate fragment formatter named '%s'", name))
	}
	fragmentFormatters[name] = constructor
}

type FragmentFormatterConstructor func(config map[string]interface{}, cache *Cache) (highlight.FragmentFormatter, error)
type FragmentFormatterRegistry map[string]FragmentFormatterConstructor
type FragmentFormatterCache map[string]highlight.FragmentFormatter

func (c FragmentFormatterCache) FragmentFormatterNamed(name string, cache *Cache) (highlight.FragmentFormatter, error) {
	fragmentFormatter, cached := c[name]
	if cached {
		return fragmentFormatter, nil
	}
	fragmentFormatterConstructor, registered := fragmentFormatters[name]
	if !registered {
		return nil, fmt.Errorf("no fragment formatter with name or type '%s' registered", name)
	}
	fragmentFormatter, err := fragmentFormatterConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building fragment formatter: %v", err)
	}
	c[name] = fragmentFormatter
	return fragmentFormatter, nil
}

func (c FragmentFormatterCache) DefineFragmentFormatter(name string, typ string, config map[string]interface{}, cache *Cache) (highlight.FragmentFormatter, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("fragment formatter named '%s' already defined", name)
	}
	fragmentFormatterConstructor, registered := fragmentFormatters[typ]
	if !registered {
		return nil, fmt.Errorf("no fragment formatter type '%s' registered", typ)
	}
	fragmentFormatter, err := fragmentFormatterConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building fragment formatter: %v", err)
	}
	c[name] = fragmentFormatter
	return fragmentFormatter, nil
}
