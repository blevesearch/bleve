//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"github.com/blevesearch/bleve/index"
)

type MatchNoneSearcher struct {
	index index.Index
}

func NewMatchNoneSearcher(index index.Index) (*MatchNoneSearcher, error) {
	return &MatchNoneSearcher{
		index: index,
	}, nil
}

func (s *MatchNoneSearcher) Count() uint64 {
	return uint64(0)
}

func (s *MatchNoneSearcher) Weight() float64 {
	return 0.0
}

func (s *MatchNoneSearcher) SetQueryNorm(qnorm float64) {

}

func (s *MatchNoneSearcher) Next() (*DocumentMatch, error) {
	return nil, nil
}

func (s *MatchNoneSearcher) Advance(ID string) (*DocumentMatch, error) {
	return nil, nil
}

func (s *MatchNoneSearcher) Close() {
}
