//  Copyright (c) 2014 Couchbase, Inc.
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

package synonym

import (
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	index "github.com/blevesearch/bleve_index_api"
)

const Name = "textsynonym"

type SynonymSource struct {
	collection string
	analyzer   string
}

func New(collection string, analyzer string) *SynonymSource {
	return &SynonymSource{
		collection: collection,
		analyzer:   analyzer,
	}
}

func (p *SynonymSource) Collection() string {
	return p.collection
}

func (p *SynonymSource) Analyzer() string {
	return p.analyzer
}

func (q *SynonymSource) MetadataKey() string {
	return q.collection + string(index.SynonymKeySeparator) + q.analyzer
}

func SynonymSourceConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.SynonymSource, error) {
	collection, ok := config["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify synonym collection")
	}
	analyzer, ok := config["analyzer"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify synonym analyzer")
	}
	return New(collection, analyzer), nil
}

func init() {
	registry.RegisterSynonymSource(Name, SynonymSourceConstructor)
}
