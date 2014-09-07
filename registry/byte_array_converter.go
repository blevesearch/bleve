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

func RegisterByteArrayConverter(name string, constructor ByteArrayConverterConstructor) {
	_, exists := byteArrayConverters[name]
	if exists {
		panic(fmt.Errorf("attempted to register duplicate byte array converter named '%s'", name))
	}
	byteArrayConverters[name] = constructor
}

type ByteArrayConverterConstructor func(config map[string]interface{}, cache *Cache) (analysis.ByteArrayConverter, error)
type ByteArrayConverterRegistry map[string]ByteArrayConverterConstructor

func ByteArrayConverterByName(name string) ByteArrayConverterConstructor {
	return byteArrayConverters[name]
}

func ByteArrayConverterTypesAndInstances() ([]string, []string) {
	emptyConfig := map[string]interface{}{}
	emptyCache := NewCache()
	types := make([]string, 0)
	instances := make([]string, 0)
	for name, cons := range byteArrayConverters {
		_, err := cons(emptyConfig, emptyCache)
		if err == nil {
			instances = append(instances, name)
		} else {
			types = append(types, name)
		}
	}
	return types, instances
}
