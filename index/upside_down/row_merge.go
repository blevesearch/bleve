//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

type dictionaryTermIncr struct{}

func newDictionaryTermIncr() *dictionaryTermIncr {
	return &dictionaryTermIncr{}
}

func (t *dictionaryTermIncr) Merge(key, existing []byte) ([]byte, error) {
	if len(existing) > 0 {
		dr, err := NewDictionaryRowKV(key, existing)
		if err != nil {
			return nil, err
		}
		dr.count++
		return dr.Value(), nil
	} else {
		dr, err := NewDictionaryRowK(key)
		if err != nil {
			return nil, err
		}
		dr.count = 1
		return dr.Value(), nil
	}
}

type dictionaryTermDecr struct{}

func newDictionaryTermDecr() *dictionaryTermDecr {
	return &dictionaryTermDecr{}
}

func (t *dictionaryTermDecr) Merge(key, existing []byte) ([]byte, error) {
	if len(existing) > 0 {
		dr, err := NewDictionaryRowKV(key, existing)
		if err != nil {
			return nil, err
		}
		dr.count--
		if dr.count > 0 {
			return dr.Value(), nil
		}
	}
	return nil, nil
}
