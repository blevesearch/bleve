//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package shredder

import (
	"github.com/couchbaselabs/bleve/document"
	"github.com/dustin/go-jsonpointer"
)

// A simple automatic JSON shredder which parses the whole document body.
// Any strings found in the JSON are added as text fields

type JsonPointerShredder struct {
	fieldPaths map[string]string
	paths      []string
}

func NewJsonPointerShredder() *JsonPointerShredder {
	return &JsonPointerShredder{
		fieldPaths: make(map[string]string),
		paths:      make([]string, 0),
	}
}

func (s *JsonPointerShredder) AddTextField(name string, path string) {
	s.fieldPaths[name] = path
	s.paths = append(s.paths, path)
}

func (s *JsonPointerShredder) AddField(name string, path string) {
	s.fieldPaths[name] = path
	s.paths = append(s.paths, path)
}

func (s *JsonPointerShredder) Shred(id string, body []byte) (*document.Document, error) {
	rv := document.NewDocument(id)

	values, err := jsonpointer.FindMany(body, s.paths)
	if err != nil {
		return nil, err
	}

	for fieldName, fieldPath := range s.fieldPaths {
		field := document.NewTextField(fieldName, values[fieldPath])
		rv.AddField(field)
	}

	return rv, nil
}
