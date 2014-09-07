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

func RegisterDateTimeParser(name string, constructor DateTimeParserConstructor) {
	_, exists := dateTimeParsers[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate date time parser named '%s'", name))
	}
	dateTimeParsers[name] = constructor
}

type DateTimeParserConstructor func(config map[string]interface{}, cache *Cache) (analysis.DateTimeParser, error)
type DateTimeParserRegistry map[string]DateTimeParserConstructor
type DateTimeParserCache map[string]analysis.DateTimeParser

func (c DateTimeParserCache) DateTimeParserNamed(name string, cache *Cache) (analysis.DateTimeParser, error) {
	dateTimeParser, cached := c[name]
	if cached {
		return dateTimeParser, nil
	}
	dateTimeParserConstructor, registered := dateTimeParsers[name]
	if !registered {
		return nil, fmt.Errorf("no date time parser with name or type '%s' registered", name)
	}
	dateTimeParser, err := dateTimeParserConstructor(nil, cache)
	if err != nil {
		return nil, fmt.Errorf("error building date time parse: %v", err)
	}
	c[name] = dateTimeParser
	return dateTimeParser, nil
}

func (c DateTimeParserCache) DefineDateTimeParser(name string, typ string, config map[string]interface{}, cache *Cache) (analysis.DateTimeParser, error) {
	_, cached := c[name]
	if cached {
		return nil, fmt.Errorf("date time parser named '%s' already defined", name)
	}
	dateTimeParserConstructor, registered := dateTimeParsers[typ]
	if !registered {
		return nil, fmt.Errorf("no date time parser type '%s' registered", typ)
	}
	dateTimeParser, err := dateTimeParserConstructor(config, cache)
	if err != nil {
		return nil, fmt.Errorf("error building date time parser: %v", err)
	}
	c[name] = dateTimeParser
	return dateTimeParser, nil
}

func DateTimeParserTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range dateTimeParsers {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
