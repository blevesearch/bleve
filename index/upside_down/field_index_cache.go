//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"sync"
)

type FieldIndexCache struct {
	fieldIndexes   map[string]uint16
	lastFieldIndex int
	mutex          sync.RWMutex
}

func NewFieldIndexCache() *FieldIndexCache {
	return &FieldIndexCache{
		fieldIndexes: make(map[string]uint16),
	}
}

func (f *FieldIndexCache) AddExisting(field string, index uint16) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.fieldIndexes[field] = index
	if int(index) > f.lastFieldIndex {
		f.lastFieldIndex = int(index)
	}
}

func (f *FieldIndexCache) FieldExists(field string) (uint16, bool) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	if index, ok := f.fieldIndexes[field]; ok {
		return index, true
	}
	return 0, false
}

func (f *FieldIndexCache) FieldIndex(field string) (uint16, *FieldRow) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	index, exists := f.fieldIndexes[field]
	if exists {
		return index, nil
	}
	// assign next field id
	index = uint16(f.lastFieldIndex + 1)
	f.fieldIndexes[field] = index
	f.lastFieldIndex = int(index)
	return index, NewFieldRow(uint16(index), field)
}

func (f *FieldIndexCache) FieldName(index uint16) string {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	for fieldName, fieldIndex := range f.fieldIndexes {
		if index == fieldIndex {
			return fieldName
		}
	}
	return ""
}
