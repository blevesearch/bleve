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
	"github.com/couchbaselabs/bleve/document"
)

type Identifier interface {
	ID() string
}

type Classifier interface {
	Type() string
}

type Index interface {
	Index(data interface{}) error
	IndexID(id string, data interface{}) error

	IndexJSON(data []byte) error
	IndexJSONID(id string, data []byte) error

	Delete(data interface{}) error
	DeleteID(id string) error

	Document(id string) (*document.Document, error)
	DocCount() uint64

	Search(req *SearchRequest) (*SearchResult, error)

	Fields() ([]string, error)

	Dump()
	DumpDoc(id string) ([]interface{}, error)
	DumpFields()

	Close()
}

// Open the index at the specified path, and create it if it does not exist.
// The provided mapping will be used for all Index/Search operations.
func Open(path string, mapping *IndexMapping) (Index, error) {
	return newIndex(path, mapping)
}
