//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"fmt"
	"github.com/blevesearch/bleve/search"
	"testing"
	"time"
)

type MockSearcher struct {
	matches []*search.DocumentMatch
}

func NewMockSearcher(matches []*search.DocumentMatch) *MockSearcher {
	return &MockSearcher{
		matches: matches,
	}
}

func (s *MockSearcher) Count() uint64 {
	return uint64(0)
}

func (s *MockSearcher) Weight() float64 {
	return 0.0
}

func (s *MockSearcher) SetQueryNorm(qnorm float64) {
}

func (s *MockSearcher) Next() (*search.DocumentMatch, error) {
	if len(s.matches) == 0 {
		return nil, nil
	}

	next := s.matches[0]
	s.matches = s.matches[1:]

	return next, nil
}

func (s *MockSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	return nil, nil
}

func (s *MockSearcher) Close() error {
	return nil
}

func (s *MockSearcher) Min() int {
	return 0
}

func TestTimeoutableSearcher(t *testing.T) {
	fakeMatches := []*search.DocumentMatch{
		&search.DocumentMatch{
			ID: "A",
		},
		&search.DocumentMatch{
			ID: "B",
		},
	}

	// Assert that a decorator with an empty timeout duration cannot be created

	mockSearcher := NewMockSearcher(fakeMatches)

	if _, err := NewTimeoutableSearcher(mockSearcher, 0); err == nil {
		t.Error("Should not be able to create a decorator with a zero value duration.")
	}

	// Assert that it's possible to iterate without errors

	timeoutableSearcher, err := NewTimeoutableSearcher(mockSearcher, time.Duration(1)*time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	next, err := timeoutableSearcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next == nil {
		t.Error("Expected DocumentMatch, got nil")
	}
	if next.ID != "A" {
		t.Error("Expected ID = A, got " + next.ID)
	}

	next, err = timeoutableSearcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next == nil {
		t.Error("Expected DocumentMatch, got nil")
	}
	if next.ID != "B" {
		t.Error("Expected ID = B, got " + next.ID)
	}

	if next, err = timeoutableSearcher.Next(); next != nil {
		t.Errorf("Expected nil, got %T", next)
	}

	// Assert that an error is given when one iterates for longer than the timeout duration

	fakeMatches = append(fakeMatches, &search.DocumentMatch{
		ID: "C",
	})

	mockSearcher = NewMockSearcher(fakeMatches)

	timeoutableSearcher, err = NewTimeoutableSearcher(mockSearcher, time.Duration(2)*time.Second)

	if err != nil {
		t.Fatal(err)
	}

	next, err = timeoutableSearcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next == nil {
		t.Error("Expected DocumentMatch, got nil")
	}
	if next.ID != "A" {
		t.Error("Expected ID = A, got " + next.ID)
	}

	time.Sleep(time.Duration(1) * time.Second)

	next, err = timeoutableSearcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next == nil {
		t.Error("Expected DocumentMatch, got nil")
	}
	if next.ID != "B" {
		t.Error("Expected ID = B, got " + next.ID)
	}

	time.Sleep(time.Duration(1) * time.Second)

	next, err = timeoutableSearcher.Next()
	if err == nil {
		t.Error("Expected TimeoutError, got nil")
	}
	if fmt.Sprintf("%T", err) != "*searchers.TimeoutError" {
		t.Errorf("Expected *searchers.TimeoutError, got %T", err)
	}
	if next != nil {
		t.Errorf("Expected nil, got %T", next)
	}
}
