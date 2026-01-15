//  Copyright (c) 2026 Couchbase, Inc.
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

// NestedFieldCache caches nested field prefixes and their corresponding nesting levels.
// A nested field prefix is a field path prefix that indicates the start of a nested document.
// The nesting level indicates how deep the nested document is in the overall document structure.
type NestedFieldCache struct {
	// nested prefix -> nested level
	prefixDepth map[string]int
	once        sync.Once
	m           sync.RWMutex
}

func NewNestedFieldCache() *NestedFieldCache {
	return &NestedFieldCache{}
}

func (nfc *NestedFieldCache) InitOnce(buildFunc func() map[string]int) {
	nfc.once.Do(func() {
		nfc.m.Lock()
		defer nfc.m.Unlock()
		nfc.prefixDepth = buildFunc()
	})
}

// NestedDepth returns two values:
//   - common: The nesting level of the longest prefix that applies to every field path
//     in the provided FieldSet. A value of 0 means no nested prefix is shared
//     across all field paths.
//   - max: The nesting level of the longest prefix that applies to at least one
//     field path in the provided FieldSet. A value of 0 means none of the
//     field paths match any nested prefix.
func (nfc *NestedFieldCache) NestedDepth(fieldPaths search.FieldSet) (common int, max int) {
	// if no field paths, no nested depth
	if len(fieldPaths) == 0 {
		return
	}
	nfc.m.RLock()
	defer nfc.m.RUnlock()
	// if no cached prefixes, no nested depth
	if len(nfc.prefixDepth) == 0 {
		return
	}
	// for each prefix, check if its a common prefix or matches any path
	// update common and max accordingly with the highest nesting level
	// possible for each respective case
	for prefix, level := range nfc.prefixDepth {
		// only check prefixes that could increase one of the results
		if level <= common && level <= max {
			continue
		}
		// check prefix against field paths, getting whether it matches all paths (common)
		// and whether it matches at least one path (any)
		matchAll, matchAny := nfc.prefixMatch(prefix, fieldPaths)
		// if it matches all paths, update common
		if matchAll && level > common {
			common = level
		}
		// if it matches any path, update max
		if matchAny && level > max {
			max = level
		}
	}
	return common, max
}

// CountNested returns the number of nested prefixes
func (nfc *NestedFieldCache) CountNested() int {
	nfc.m.RLock()
	defer nfc.m.RUnlock()

	return len(nfc.prefixDepth)
}

// IntersectsPrefix returns true if any of the given
// field paths have a nested prefix
func (nfc *NestedFieldCache) IntersectsPrefix(fieldPaths search.FieldSet) bool {
	// if no field paths, no intersection
	if len(fieldPaths) == 0 {
		return false
	}
	nfc.m.RLock()
	defer nfc.m.RUnlock()
	// if no cached prefixes, no intersection
	if len(nfc.prefixDepth) == 0 {
		return false
	}
	// Check each cached nested prefix to see if it intersects with any path
	for prefix := range nfc.prefixDepth {
		_, matchAny := nfc.prefixMatch(prefix, fieldPaths)
		if matchAny {
			return true
		}
	}
	return false
}

// prefixMatch checks whether the prefix matches all paths (common) and whether it matches at least one path (any)
// Caller must hold the read lock.
func (nfc *NestedFieldCache) prefixMatch(prefix string, fieldPaths search.FieldSet) (common bool, any bool) {
	common = true
	any = false
	for path := range fieldPaths {
		has := strings.HasPrefix(path, prefix)
		if has {
			any = true
		} else {
			common = false
		}
		// early exit if we have determined both values
		if any && !common {
			break
		}
	}
	return common, any
}
