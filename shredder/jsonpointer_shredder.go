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
	"bytes"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/document"

	"github.com/dustin/go-jsonpointer"
)

// A simple automatic JSON shredder which parses the whole document body.
// Any strings found in the JSON are added as text fields

type JsonPointerShredder struct {
	fieldPaths map[string]string
	paths      []string
	analyzers  map[string]*analysis.Analyzer
	options    map[string]document.IndexingOptions
}

func NewJsonPointerShredder() *JsonPointerShredder {
	return &JsonPointerShredder{
		fieldPaths: make(map[string]string),
		paths:      make([]string, 0),
		analyzers:  make(map[string]*analysis.Analyzer),
		options:    make(map[string]document.IndexingOptions),
	}
}

func (s *JsonPointerShredder) AddTextField(name string, path string) {
	s.fieldPaths[name] = path
	s.paths = append(s.paths, path)
}

func (s *JsonPointerShredder) AddFieldCustom(name string, path string, options document.IndexingOptions, analyzer *analysis.Analyzer) {
	s.fieldPaths[name] = path
	s.analyzers[name] = analyzer
	s.options[name] = options
	s.paths = append(s.paths, path)
}

func (s *JsonPointerShredder) Shred(id string, body []byte) (*document.Document, error) {
	rv := document.NewDocument(id)

	values, err := jsonpointer.FindMany(body, s.paths)
	if err != nil {
		return nil, err
	}

	for fieldName, fieldPath := range s.fieldPaths {
		fieldValue := bytes.TrimSpace(values[fieldPath])
		if bytes.HasPrefix(fieldValue, []byte{'"'}) {
			fieldValue = fieldValue[1:]
		}
		if bytes.HasSuffix(fieldValue, []byte{'"'}) {
			fieldValue = fieldValue[:len(fieldValue)-1]
		}
		analyzer, custom := s.analyzers[fieldName]
		if custom {
			options := s.options[fieldName]
			field := document.NewTextFieldCustom(fieldName, fieldValue, options, analyzer)
			rv.AddField(field)
		} else {
			field := document.NewTextField(fieldName, fieldValue)
			rv.AddField(field)
		}
	}

	return rv, nil
}
