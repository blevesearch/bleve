//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package collectors

import (
	"github.com/blevesearch/bleve/search"
)

type stubSearcher struct {
	index   int
	matches search.DocumentMatchCollection
}

func (ss *stubSearcher) Next() (*search.DocumentMatch, error) {
	if ss.index < len(ss.matches) {
		rv := ss.matches[ss.index]
		ss.index++
		return rv, nil
	}
	return nil, nil
}

func (ss *stubSearcher) Advance(ID string) (*search.DocumentMatch, error) {

	for ss.index < len(ss.matches) && ss.matches[ss.index].ID < ID {
		ss.index++
	}
	if ss.index < len(ss.matches) {
		rv := ss.matches[ss.index]
		ss.index++
		return rv, nil
	}
	return nil, nil
}

func (ss *stubSearcher) Close() error {
	return nil
}

func (ss *stubSearcher) Weight() float64 {
	return 0.0
}

func (ss *stubSearcher) SetQueryNorm(float64) {
}

func (ss *stubSearcher) Count() uint64 {
	return uint64(len(ss.matches))
}

func (ss *stubSearcher) Min() int {
	return 0
}
