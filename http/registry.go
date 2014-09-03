//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package http

import (
	"sync"

	"github.com/blevesearch/bleve"
)

var indexNameMapping map[string]bleve.Index
var indexNameMappingLock sync.RWMutex

func RegisterIndexName(name string, index bleve.Index) {
	indexNameMappingLock.Lock()
	defer indexNameMappingLock.Unlock()

	if indexNameMapping == nil {
		indexNameMapping = make(map[string]bleve.Index)
	}
	indexNameMapping[name] = index
}

func UnregisterIndexByName(name string) bleve.Index {
	indexNameMappingLock.Lock()
	defer indexNameMappingLock.Unlock()

	if indexNameMapping == nil {
		return nil
	}
	rv := indexNameMapping[name]
	if rv != nil {
		delete(indexNameMapping, name)
	}
	return rv
}

func IndexByName(name string) bleve.Index {
	indexNameMappingLock.RLock()
	defer indexNameMappingLock.RUnlock()

	return indexNameMapping[name]
}

func IndexNames() []string {
	indexNameMappingLock.RLock()
	defer indexNameMappingLock.RUnlock()

	rv := make([]string, len(indexNameMapping))
	count := 0
	for k := range indexNameMapping {
		rv[count] = k
		count++
	}
	return rv
}
