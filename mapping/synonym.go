//  Copyright (c) 2024 Couchbase, Inc.
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

package mapping

import (
	"fmt"

	"github.com/blevesearch/bleve/v2/registry"
)

type SynonymSource struct {
	CollectionName string `json:"collection"`
	AnalyzerName   string `json:"analyzer"`
}

func NewSynonymSource(collection, analyzer string) *SynonymSource {
	return &SynonymSource{
		CollectionName: collection,
		AnalyzerName:   analyzer,
	}
}

func (s *SynonymSource) Collection() string {
	return s.CollectionName
}

func (s *SynonymSource) Analyzer() string {
	return s.AnalyzerName
}

func (s *SynonymSource) SetCollection(c string) {
	s.CollectionName = c
}

func (s *SynonymSource) SetAnalyzer(a string) {
	s.AnalyzerName = a
}

func (s *SynonymSource) Validate(c *registry.Cache) error {
	if s.CollectionName == "" {
		return fmt.Errorf("collection name is required")
	}
	if s.AnalyzerName == "" {
		return fmt.Errorf("analyzer name is required")
	}
	_, err := c.AnalyzerNamed(s.AnalyzerName)
	if err != nil {
		return fmt.Errorf("analyzer named '%s' not found", s.AnalyzerName)
	}
	return nil
}
