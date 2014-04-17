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
	"encoding/json"
	"strconv"

	"github.com/couchbaselabs/bleve/document"
)

// A simple automatic JSON shredder which parses the whole document body.
// Any strings found in the JSON are added as text fields

type AutoJsonShredder struct {
}

func NewAutoJsonShredder() *AutoJsonShredder {
	return &AutoJsonShredder{}
}

func (s *AutoJsonShredder) Shred(id string, body []byte) (*document.Document, error) {
	rv := document.NewDocument(id)

	var section interface{}
	err := json.Unmarshal(body, &section)
	if err != nil {
		return nil, err
	}

	shredSection(rv, section, "")

	return rv, nil
}

func shredSection(doc *document.Document, section interface{}, parent string) {
	nextParent := parent
	if nextParent != "" {
		nextParent = nextParent + "."
	}
	switch section := section.(type) {

	case string:
		f := document.NewTextField(parent, []byte(section))
		doc.AddField(f)

	case []interface{}:
		for i, sub := range section {
			shredSection(doc, sub, nextParent+strconv.Itoa(i))
		}

	case map[string]interface{}:
		for k, sub := range section {
			shredSection(doc, sub, nextParent+k)
		}
	}
}
