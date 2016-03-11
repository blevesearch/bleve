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
	"fmt"
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

	subDocMappingB := NewDocumentMapping()
	customFieldX := NewTextFieldMapping()
	customFieldX.Analyzer = "analyzerx"
	subDocMappingB.AddFieldMappingsAt("desc", customFieldX)

	docMappingA.AddFieldMappingsAt("author", enFieldMapping, customMapping)
	docMappingA.AddSubDocumentMapping("child", subDocMappingB)

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

	analyzerName = mapping.analyzerNameForPath("child.desc")
	if analyzerName != customFieldX.Analyzer {
		t.Errorf("expected '%s' got '%s'", customFieldX.Analyzer, analyzerName)
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

func TestEnablingDisablingStoringDynamicFields(t *testing.T) {

	// first verify that with system defaults, dynamic field is stored
	data := map[string]interface{}{
		"name": "bleve",
	}
	doc := document.NewDocument("x")
	mapping := NewIndexMapping()
	err := mapping.mapDocument(doc, data)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range doc.Fields {
		if field.Name() == "name" && !field.Options().IsStored() {
			t.Errorf("expected field 'name' to be stored, isn't")
		}
	}

	// now change system level defaults, verify dynamic field is not stored
	StoreDynamic = false
	defer func() {
		StoreDynamic = true
	}()

	mapping = NewIndexMapping()
	doc = document.NewDocument("y")
	err = mapping.mapDocument(doc, data)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range doc.Fields {
		if field.Name() == "name" && field.Options().IsStored() {
			t.Errorf("expected field 'name' to be not stored, is")
		}
	}

	// now override the system level defaults inside the index mapping
	mapping = NewIndexMapping()
	mapping.StoreDynamic = true
	doc = document.NewDocument("y")
	err = mapping.mapDocument(doc, data)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range doc.Fields {
		if field.Name() == "name" && !field.Options().IsStored() {
			t.Errorf("expected field 'name' to be stored, isn't")
		}
	}
}

func TestMappingBool(t *testing.T) {
	boolMapping := NewBooleanFieldMapping()
	docMapping := NewDocumentMapping()
	docMapping.AddFieldMappingsAt("prop", boolMapping)
	mapping := NewIndexMapping()
	mapping.AddDocumentMapping("doc", docMapping)

	pprop := false
	x := struct {
		Prop  bool  `json:"prop"`
		PProp *bool `json:"pprop"`
	}{
		Prop:  true,
		PProp: &pprop,
	}

	doc := document.NewDocument("1")
	err := mapping.mapDocument(doc, x)
	if err != nil {
		t.Fatal(err)
	}
	foundProp := false
	foundPProp := false
	count := 0
	for _, f := range doc.Fields {
		if f.Name() == "prop" {
			foundProp = true
		}
		if f.Name() == "pprop" {
			foundPProp = true
		}
		count++
	}
	if !foundProp {
		t.Errorf("expected to find bool field named 'prop'")
	}
	if !foundPProp {
		t.Errorf("expected to find pointer to bool field named 'pprop'")
	}
	if count != 2 {
		t.Errorf("expected to find 2 fields, found %d", count)
	}
}

func TestDisableDefaultMapping(t *testing.T) {
	indexMapping := NewIndexMapping()
	indexMapping.DefaultMapping.Enabled = false

	data := map[string]string{
		"name": "bleve",
	}

	doc := document.NewDocument("x")
	err := indexMapping.mapDocument(doc, data)
	if err != nil {
		t.Error(err)
	}

	if len(doc.Fields) > 0 {
		t.Errorf("expected no fields, got %d", len(doc.Fields))
	}
}

func TestInvalidFieldMappingStrict(t *testing.T) {
	mappingBytes := []byte(`{"includeInAll":true,"name":"a parsed name"}`)

	// first unmarhsal it without strict
	var fm FieldMapping
	err := json.Unmarshal(mappingBytes, &fm)
	if err != nil {
		t.Fatal(err)
	}

	if fm.Name != "a parsed name" {
		t.Fatalf("expect to find field mapping name 'a parsed name', got '%s'", fm.Name)
	}

	// reset
	fm.Name = ""

	// now enable strict
	MappingJSONStrict = true
	defer func() {
		MappingJSONStrict = false
	}()

	expectedInvalidKeys := []string{"includeInAll"}
	expectedErr := fmt.Errorf("field mapping contains invalid keys: %v", expectedInvalidKeys)
	err = json.Unmarshal(mappingBytes, &fm)
	if err.Error() != expectedErr.Error() {
		t.Fatalf("expected err: %v, got err: %v", expectedErr, err)
	}

	if fm.Name != "a parsed name" {
		t.Fatalf("expect to find field mapping name 'a parsed name', got '%s'", fm.Name)
	}

}

func TestInvalidDocumentMappingStrict(t *testing.T) {
	mappingBytes := []byte(`{"defaultAnalyzer":true,"enabled":false}`)

	// first unmarhsal it without strict
	var dm DocumentMapping
	err := json.Unmarshal(mappingBytes, &dm)
	if err != nil {
		t.Fatal(err)
	}

	if dm.Enabled != false {
		t.Fatalf("expect to find document mapping enabled false, got '%t'", dm.Enabled)
	}

	// reset
	dm.Enabled = true

	// now enable strict
	MappingJSONStrict = true
	defer func() {
		MappingJSONStrict = false
	}()

	expectedInvalidKeys := []string{"defaultAnalyzer"}
	expectedErr := fmt.Errorf("document mapping contains invalid keys: %v", expectedInvalidKeys)
	err = json.Unmarshal(mappingBytes, &dm)
	if err.Error() != expectedErr.Error() {
		t.Fatalf("expected err: %v, got err: %v", expectedErr, err)
	}

	if dm.Enabled != false {
		t.Fatalf("expect to find document mapping enabled false, got '%t'", dm.Enabled)
	}
}

func TestInvalidIndexMappingStrict(t *testing.T) {
	mappingBytes := []byte(`{"typeField":"type","default_field":"all"}`)

	// first unmarhsal it without strict
	var im IndexMapping
	err := json.Unmarshal(mappingBytes, &im)
	if err != nil {
		t.Fatal(err)
	}

	if im.DefaultField != "all" {
		t.Fatalf("expect to find index mapping default field 'all', got '%s'", im.DefaultField)
	}

	// reset
	im.DefaultField = "_all"

	// now enable strict
	MappingJSONStrict = true
	defer func() {
		MappingJSONStrict = false
	}()

	expectedInvalidKeys := []string{"typeField"}
	expectedErr := fmt.Errorf("index mapping contains invalid keys: %v", expectedInvalidKeys)
	err = json.Unmarshal(mappingBytes, &im)
	if err.Error() != expectedErr.Error() {
		t.Fatalf("expected err: %v, got err: %v", expectedErr, err)
	}

	if im.DefaultField != "all" {
		t.Fatalf("expect to find index mapping default field 'all', got '%s'", im.DefaultField)
	}
}

func TestMappingBug353(t *testing.T) {
	dataBytes := `{
  "Reviews": [
    {
      "ReviewID": "RX16692001",
      "Content": "Usually stay near the airport..."
    }
	],
	"Other": {
	  "Inside": "text"
  },
  "Name": "The Inn at Baltimore White Marsh"
}`

	var data map[string]interface{}
	err := json.Unmarshal([]byte(dataBytes), &data)
	if err != nil {
		t.Fatal(err)
	}

	reviewContentFieldMapping := NewTextFieldMapping()
	reviewContentFieldMapping.Analyzer = "crazy"

	reviewsMapping := NewDocumentMapping()
	reviewsMapping.Dynamic = false
	reviewsMapping.AddFieldMappingsAt("Content", reviewContentFieldMapping)
	otherMapping := NewDocumentMapping()
	otherMapping.Dynamic = false
	mapping := NewIndexMapping()
	mapping.DefaultMapping.AddSubDocumentMapping("Reviews", reviewsMapping)
	mapping.DefaultMapping.AddSubDocumentMapping("Other", otherMapping)

	doc := document.NewDocument("x")
	err = mapping.mapDocument(doc, data)
	if err != nil {
		t.Fatal(err)
	}

	// expect doc has only 2 fields
	if len(doc.Fields) != 2 {
		t.Errorf("expected doc with 2 fields, got: %d", len(doc.Fields))
		for _, f := range doc.Fields {
			t.Logf("field named: %s", f.Name())
		}
	}
}
