//  Copyright (c) 2025 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package registry

import (
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/search"
)

type NestedFieldCache struct {
	// nested prefix -> nested level
	c *ConcurrentCache

	once sync.Once
}

func NewNestedFieldCache() *NestedFieldCache {
	return &NestedFieldCache{
		NewConcurrentCache(),
		sync.Once{},
	}
}

func (nfc *NestedFieldCache) InitOnce(initFunc func()) {
	nfc.once.Do(initFunc)
}

func (nfc *NestedFieldCache) AddPrefix(prefix string, level int) error {
	buildFunc := func(name string, config map[string]interface{}, cache *Cache) (interface{}, error) {
		return level, nil
	}
	_, err := nfc.c.DefineItem(prefix, "", nil, nil, buildFunc)
	if err == ErrAlreadyDefined {
		// Already exists, that's ok
		return nil
	}
	return err
}

// Returns the deepest nested level that covers all the given field paths
func (nfc *NestedFieldCache) CoveringDepth(fieldPaths search.FieldSet) int {
	if len(fieldPaths) == 0 {
		return 0
	}

	nfc.c.mutex.RLock()
	defer nfc.c.mutex.RUnlock()

	deepestLevel := 0

	// Check each cached nested prefix
	for prefix, item := range nfc.c.data {
		level, ok := item.(int)
		if !ok {
			continue
		}

		// Check if this nested prefix belongs to all the given paths
		isCommonPrefix := true
		for path := range fieldPaths {
			if !strings.HasPrefix(path, prefix) {
				isCommonPrefix = false
				break
			}
		}

		// If it's a common prefix and deeper than what we've found so far
		if isCommonPrefix && level > deepestLevel {
			deepestLevel = level
		}
	}

	return deepestLevel
}

func (nfc *NestedFieldCache) CountNested() int {
	nfc.c.mutex.RLock()
	defer nfc.c.mutex.RUnlock()

	return len(nfc.c.data)
}

func (nfc *NestedFieldCache) IntersectsPrefix(fieldPaths search.FieldSet) bool {
	nfc.c.mutex.RLock()
	defer nfc.c.mutex.RUnlock()
	for prefix := range nfc.c.data {
		for path := range fieldPaths {
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}
	}
	return false
}
