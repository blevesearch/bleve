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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/analysis/tokenizers/exception"
	"github.com/blevesearch/bleve/analysis/tokenizers/regexp_tokenizer"
	"github.com/blevesearch/bleve/document"
)

var mappingSource = []byte(`{
    "types": {
    	"beer": {
    		"properties": {
    			"name": {
    				"fields": [
    					{
    						"name": "name",
    						"type": "text",
    						"analyzer": "standard",
    						"store": true,
    						"index": true,
                            "include_term_vectors": true,
                            "include_in_all": true
    					}
    				]
    			}
    		}
    	},
    	"brewery": {
    	}
    },
    "type_field": "_type",
    "default_type": "_default"
}`)

func buildMapping() *IndexMapping {
	nameFieldMapping := NewTextFieldMapping()
	nameFieldMapping.Name = "name"
	nameFieldMapping.Analyzer = "standard"

	beerMapping := NewDocumentMapping()
	beerMapping.AddFieldMappingsAt("name", nameFieldMapping)

	breweryMapping := NewDocumentMapping()

	mapping := NewIndexMapping()
	mapping.AddDocumentMapping("beer", beerMapping)
	mapping.AddDocumentMapping("brewery", breweryMapping)

	return mapping
}

func TestUnmarshalMappingJSON(t *testing.T) {
	mapping := buildMapping()

	var indexMapping IndexMapping
	err := json.Unmarshal(mappingSource, &indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&indexMapping, mapping) {
		t.Errorf("expected %#v,\n got %#v", mapping, &indexMapping)
	}
}

func TestMappingStructWithJSONTags(t *testing.T) {

	mapping := buildMapping()

	x := struct {
		NoJSONTag string
		Name      string `json:"name"`
	}{
		Name: "marty",
	}

	doc := document.NewDocument("1")
	err := mapping.mapDocument(doc, x)
	if err != nil {
		t.Fatal(err)
	}
	foundJSONName := false
	foundNoJSONName := false
	count := 0
	for _, f := range doc.Fields {
		if f.Name() == "name" {
			foundJSONName = true
		}
		if f.Name() == "NoJSONTag" {
			foundNoJSONName = true
		}
		count++
	}
	if !foundJSONName {
		t.Errorf("expected to find field named 'name'")
	}
	if !foundNoJSONName {
		t.Errorf("expected to find field named 'NoJSONTag'")
	}
	if count != 2 {
		t.Errorf("expected to find 2 find, found %d", count)
	}
}

func TestMappingStructWithJSONTagsOneDisabled(t *testing.T) {

	mapping := buildMapping()

	x := struct {
		Name      string `json:"name"`
		Title     string `json:"-"`
		NoJSONTag string
	}{
		Name: "marty",
	}

	doc := document.NewDocument("1")
	err := mapping.mapDocument(doc, x)
	if err != nil {
		t.Fatal(err)
	}
	foundJSONName := false
	foundNoJSONName := false
	count := 0
	for _, f := range doc.Fields {
		if f.Name() == "name" {
			foundJSONName = true
		}
		if f.Name() == "NoJSONTag" {
			foundNoJSONName = true
		}
		count++
	}
	if !foundJSONName {
		t.Errorf("expected to find field named 'name'")
	}
	if !foundNoJSONName {
		t.Errorf("expected to find field named 'NoJSONTag'")
	}
	if count != 2 {
		t.Errorf("expected to find 2 find, found %d", count)
	}
}

func TestMappingStructWithPointerToString(t *testing.T) {

	mapping := buildMapping()

	name := "marty"

	x := struct {
		Name *string
	}{
		Name: &name,
	}

	doc := document.NewDocument("1")
	err := mapping.mapDocument(doc, x)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	count := 0
	for _, f := range doc.Fields {
		if f.Name() == "Name" {
			found = true
		}
		count++
	}
	if !found {
		t.Errorf("expected to find field named 'Name'")
	}
	if count != 1 {
		t.Errorf("expected to find 1 find, found %d", count)
	}
}

func TestMappingJSONWithNull(t *testing.T) {

	mapping := NewIndexMapping()

	jsonbytes := []byte(`{"name":"marty", "age": null}`)
	var jsondoc interface{}
	err := json.Unmarshal(jsonbytes, &jsondoc)
	if err != nil {
		t.Fatal(err)
	}

	doc := document.NewDocument("1")
	err = mapping.mapDocument(doc, jsondoc)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	count := 0
	for _, f := range doc.Fields {
		if f.Name() == "name" {
			found = true
		}
		count++
	}
	if !found {
		t.Errorf("expected to find field named 'name'")
	}
	if count != 1 {
		t.Errorf("expected to find 1 find, found %d", count)
	}
}

func TestMappingForPath(t *testing.T) {

	enFieldMapping := NewTextFieldMapping()
	enFieldMapping.Analyzer = "en"

	docMappingA := NewDocumentMapping()
	docMappingA.AddFieldMappingsAt("name", enFieldMapping)

	customMapping := NewTextFieldMapping()
	customMapping.Analyzer = "xyz"
	customMapping.Name = "nameCustom"

	docMappingA.AddFieldMappingsAt("author", enFieldMapping, customMapping)

	mapping := NewIndexMapping()
	mapping.AddDocumentMapping("a", docMappingA)

	analyzerName := mapping.analyzerNameForPath("name")
	if analyzerName != enFieldMapping.Analyzer {
		t.Errorf("expected '%s' got '%s'", enFieldMapping.Analyzer, analyzerName)
	}

	analyzerName = mapping.analyzerNameForPath("nameCustom")
	if analyzerName != customMapping.Analyzer {
		t.Errorf("expected '%s' got '%s'", customMapping.Analyzer, analyzerName)
	}

}

func TestMappingWithTokenizerDeps(t *testing.T) {

	tokNoDeps := map[string]interface{}{
		"type":   regexp_tokenizer.Name,
		"regexp": "",
	}

	tokDepsL1 := map[string]interface{}{
		"type":       exception.Name,
		"tokenizer":  "a",
		"exceptions": []string{".*"},
	}

	// this tests a 1-level dependency
	// it is run 100 times to increase the
	// likelihood that it fails along time way
	// (depends on key order iteration in map)
	for i := 0; i < 100; i++ {

		m := NewIndexMapping()
		ca := customAnalysis{
			Tokenizers: map[string]map[string]interface{}{
				"a": tokNoDeps,
				"b": tokDepsL1,
			},
		}
		err := ca.registerAll(m)
		if err != nil {
			t.Fatal(err)
		}
	}

	tokDepsL2 := map[string]interface{}{
		"type":       "exception",
		"tokenizer":  "b",
		"exceptions": []string{".*"},
	}

	// now test a second-level dependency
	for i := 0; i < 100; i++ {

		m := NewIndexMapping()
		ca := customAnalysis{
			Tokenizers: map[string]map[string]interface{}{
				"a": tokNoDeps,
				"b": tokDepsL1,
				"c": tokDepsL2,
			},
		}
		err := ca.registerAll(m)
		if err != nil {
			t.Fatal(err)
		}
	}

	tokUnsatisfied := map[string]interface{}{
		"type":      "exception",
		"tokenizer": "e",
	}

	// now make sure an unsatisfied dep still
	// results in an error
	m := NewIndexMapping()
	ca := customAnalysis{
		Tokenizers: map[string]map[string]interface{}{
			"a": tokNoDeps,
			"b": tokDepsL1,
			"c": tokDepsL2,
			"d": tokUnsatisfied,
		},
	}
	err := ca.registerAll(m)
	if err == nil {
		t.Fatal(err)
	}
}
