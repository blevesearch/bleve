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
	"fmt"

	"github.com/couchbaselabs/bleve/document"
)

type FieldMapping struct {
	Name               *string `json:"name"`
	Type               *string `json:"type"`
	Analyzer           *string `json:"analyzer"`
	Store              *bool   `json:"store"`
	Index              *bool   `json:"index"`
	IncludeTermVectors *bool   `json:"include_term_vectors"`
	IncludeInAll       *bool   `json:"include_in_all"`
}

func NewFieldMapping(name, typ, analyzer string, store, index bool, includeTermVectors bool, includeInAll bool) *FieldMapping {
	return &FieldMapping{
		Name:               &name,
		Type:               &typ,
		Analyzer:           &analyzer,
		Store:              &store,
		Index:              &index,
		IncludeTermVectors: &includeTermVectors,
		IncludeInAll:       &includeInAll,
	}
}

func (fm *FieldMapping) Options() document.IndexingOptions {
	var rv document.IndexingOptions
	if *fm.Store {
		rv |= document.STORE_FIELD
	}
	if *fm.Index {
		rv |= document.INDEX_FIELD
	}
	if *fm.IncludeTermVectors {
		rv |= document.INCLUDE_TERM_VECTORS
	}
	return rv
}

func (fm *FieldMapping) GoString() string {
	return fmt.Sprintf("&bleve.FieldMapping{Name:%s, Type:%s, Analyzer:%s, Store:%t, Index:%t}", *fm.Name, *fm.Type, *fm.Analyzer, *fm.Store, *fm.Index)
}
