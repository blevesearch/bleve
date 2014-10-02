//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"encoding/json"
	"sync/atomic"
)

type IndexStat struct {
	indexStat  json.Marshaler
	searches   uint64
	searchTime uint64
}

func (is *IndexStat) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	m["index"] = is.indexStat
	m["searches"] = atomic.LoadUint64(&is.searches)
	m["search_time"] = atomic.LoadUint64(&is.searchTime)
	return json.Marshal(m)
}

type IndexStats map[string]*IndexStat

func (i IndexStats) String() string {
	bytes, err := json.Marshal(i)
	if err != nil {
		return "error marshaling stats"
	}
	return string(bytes)
}
