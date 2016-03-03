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
	"github.com/blevesearch/bleve/search"
	"time"
)

type TimeoutError struct {
	msg string
}

func (e *TimeoutError) Error() string { return e.msg }

type TimeoutableSearcher struct {
	decorated    search.Searcher
	timeoutAfter time.Duration
	timeoutAt    time.Time
}

func NewTimeoutableSearcher(decorated search.Searcher, timeoutAfter time.Duration) (*TimeoutableSearcher, error) {
	if timeoutAfter == 0 {
		return nil, &TimeoutError{"Must set timeout duration."}
	}
	return &TimeoutableSearcher{
		decorated:    decorated,
		timeoutAfter: timeoutAfter,
		timeoutAt:    time.Time{},
	}, nil
}

func (s *TimeoutableSearcher) Weight() float64 {
	return s.decorated.Weight()
}

func (s *TimeoutableSearcher) SetQueryNorm(qnorm float64) {
	s.decorated.SetQueryNorm(qnorm)
}

func (s *TimeoutableSearcher) Next() (*search.DocumentMatch, error) {
	if s.timeoutAt.IsZero() {
		s.timeoutAt = time.Now().Add(s.timeoutAfter)
	} else if time.Now().After(s.timeoutAt) {
		return nil, &TimeoutError{"Query timed out."}
	}
	return s.decorated.Next()
}

func (s *TimeoutableSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return s.decorated.Advance(ID)
}

func (s *TimeoutableSearcher) Count() uint64 {
	return s.decorated.Count()
}

func (s *TimeoutableSearcher) Close() error {
	return s.decorated.Close()
}

func (s *TimeoutableSearcher) Min() int {
	return s.decorated.Min()
}
