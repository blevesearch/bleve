//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type MatchNoneSearcher struct {
	indexReader index.IndexReader
}

func NewMatchNoneSearcher(indexReader index.IndexReader) (*MatchNoneSearcher, error) {
	return &MatchNoneSearcher{
		indexReader: indexReader,
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

func (s *MatchNoneSearcher) Next(preAllocated *search.DocumentMatch) (*search.DocumentMatch, error) {
	return nil, nil
}

func (s *MatchNoneSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return nil, nil
}

func (s *MatchNoneSearcher) Close() error {
	return nil
}

func (s *MatchNoneSearcher) Min() int {
	return 0
}
