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
	"os"
	"testing"
)

type Address struct {
	Street string `json:"street"`
	City   string `json:"city"`
	State  string `json:"state"`
	Zip    string `json:"zip"`
}

type Person struct {
	Identifier string     `json:"id"`
	Name       string     `json:"name"`
	Address    *Address   `json:"address"`
	Hideouts   []*Address `json:"hideouts"`
	Tags       []string   `json:"tags"`
}

func (p *Person) ID() string {
	return p.Identifier
}

func (p *Person) Type() string {
	return "person"
}

// FIXME needs more assertions
func TestIndex(t *testing.T) {
	defer os.RemoveAll("testidx")

	nameMapping := NewDocumentMapping().
		AddFieldMapping(NewFieldMapping("", "text", "standard", true, true, true, true))

	tagsMapping := NewDocumentMapping().
		AddFieldMapping(NewFieldMapping("", "text", "standard", true, true, true, false))
	personMapping := NewDocumentMapping().
		AddSubDocumentMapping("name", nameMapping).
		AddSubDocumentMapping("id", NewDocumentDisabledMapping()).
		AddSubDocumentMapping("tags", tagsMapping)

	mapping := NewIndexMapping().
		AddDocumentMapping("person", personMapping)
	index, err := Open("testidx", mapping)
	if err != nil {
		t.Fatal(err)
	}

	obj := Person{
		Identifier: "a",
		Name:       "marty",
		Address: &Address{
			Street: "123 Sesame St.",
			City:   "Garden",
			State:  "MIND",
			Zip:    "12345",
		},
		Hideouts: []*Address{
			&Address{
				Street: "999 Gopher St.",
				City:   "Denver",
				State:  "CO",
				Zip:    "86753",
			},
			&Address{
				Street: "88 Rusty Ln.",
				City:   "Amsterdam",
				State:  "CA",
				Zip:    "09090",
			},
		},
		Tags: []string{"amped", "bogus", "gnarley", "tubed"},
	}

	err = index.Index(&obj)
	if err != nil {
		t.Error(err)
	}
}
