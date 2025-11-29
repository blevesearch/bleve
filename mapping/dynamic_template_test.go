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

package mapping

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
)

func TestDynamicTemplateMatching(t *testing.T) {
	tests := []struct {
		name         string
		template     *DynamicTemplate
		fieldName    string
		pathStr      string
		detectedType string
		shouldMatch  bool
	}{
		{
			name: "match field name with wildcard suffix",
			template: &DynamicTemplate{
				Match: "*_text",
			},
			fieldName:    "title_text",
			pathStr:      "title_text",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "match field name with wildcard prefix",
			template: &DynamicTemplate{
				Match: "field_*",
			},
			fieldName:    "field_name",
			pathStr:      "field_name",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "no match field name",
			template: &DynamicTemplate{
				Match: "*_text",
			},
			fieldName:    "title_keyword",
			pathStr:      "title_keyword",
			detectedType: "string",
			shouldMatch:  false,
		},
		{
			name: "match path with double star",
			template: &DynamicTemplate{
				PathMatch: "metadata.**",
			},
			fieldName:    "author",
			pathStr:      "metadata.author",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "match nested path with double star",
			template: &DynamicTemplate{
				PathMatch: "metadata.**",
			},
			fieldName:    "primary",
			pathStr:      "metadata.tags.primary",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "no match path",
			template: &DynamicTemplate{
				PathMatch: "metadata.**",
			},
			fieldName:    "name",
			pathStr:      "content.name",
			detectedType: "string",
			shouldMatch:  false,
		},
		{
			name: "match mapping type",
			template: &DynamicTemplate{
				MatchMappingType: "string",
			},
			fieldName:    "anything",
			pathStr:      "anything",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "no match mapping type",
			template: &DynamicTemplate{
				MatchMappingType: "number",
			},
			fieldName:    "anything",
			pathStr:      "anything",
			detectedType: "string",
			shouldMatch:  false,
		},
		{
			name: "match with unmatch exclusion",
			template: &DynamicTemplate{
				Match:   "*_field",
				Unmatch: "skip_*",
			},
			fieldName:    "title_field",
			pathStr:      "title_field",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "excluded by unmatch",
			template: &DynamicTemplate{
				Match:   "*_field",
				Unmatch: "skip_*",
			},
			fieldName:    "skip_field",
			pathStr:      "skip_field",
			detectedType: "string",
			shouldMatch:  false,
		},
		{
			name: "match with path_unmatch exclusion",
			template: &DynamicTemplate{
				PathMatch:   "data.**",
				PathUnmatch: "data.internal.**",
			},
			fieldName:    "value",
			pathStr:      "data.public.value",
			detectedType: "string",
			shouldMatch:  true,
		},
		{
			name: "excluded by path_unmatch",
			template: &DynamicTemplate{
				PathMatch:   "data.**",
				PathUnmatch: "data.internal.**",
			},
			fieldName:    "secret",
			pathStr:      "data.internal.secret",
			detectedType: "string",
			shouldMatch:  false,
		},
		{
			name: "combined match criteria",
			template: &DynamicTemplate{
				Match:            "*_count",
				MatchMappingType: "number",
			},
			fieldName:    "item_count",
			pathStr:      "item_count",
			detectedType: "number",
			shouldMatch:  true,
		},
		{
			name: "combined match - type mismatch",
			template: &DynamicTemplate{
				Match:            "*_count",
				MatchMappingType: "number",
			},
			fieldName:    "item_count",
			pathStr:      "item_count",
			detectedType: "string", // type doesn't match
			shouldMatch:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.template.Matches(tt.fieldName, tt.pathStr, tt.detectedType)
			if result != tt.shouldMatch {
				t.Errorf("expected match=%v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestDetectMappingType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"string", "hello", "string"},
		{"int", 42, "number"},
		{"int64", int64(42), "number"},
		{"float64", 3.14, "number"},
		{"bool", true, "boolean"},
		{"time", time.Now(), "date"},
		{"map", map[string]interface{}{}, "object"},
		{"slice", []string{}, "array"},
		{"nil pointer", (*string)(nil), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			result := detectMappingType(val)
			if result != tt.expected {
				t.Errorf("expected type=%s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDynamicTemplateJSON(t *testing.T) {
	jsonData := `{
		"name": "keyword_fields",
		"match": "*_keyword",
		"unmatch": "skip_*",
		"path_match": "data.**",
		"path_unmatch": "data.internal.**",
		"match_mapping_type": "string",
		"mapping": {
			"type": "text",
			"analyzer": "keyword",
			"store": true,
			"index": true
		}
	}`

	var template DynamicTemplate
	err := json.Unmarshal([]byte(jsonData), &template)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if template.Name != "keyword_fields" {
		t.Errorf("expected name=keyword_fields, got %s", template.Name)
	}
	if template.Match != "*_keyword" {
		t.Errorf("expected match=*_keyword, got %s", template.Match)
	}
	if template.Unmatch != "skip_*" {
		t.Errorf("expected unmatch=skip_*, got %s", template.Unmatch)
	}
	if template.PathMatch != "data.**" {
		t.Errorf("expected path_match=data.**, got %s", template.PathMatch)
	}
	if template.PathUnmatch != "data.internal.**" {
		t.Errorf("expected path_unmatch=data.internal.**, got %s", template.PathUnmatch)
	}
	if template.MatchMappingType != "string" {
		t.Errorf("expected match_mapping_type=string, got %s", template.MatchMappingType)
	}
	if template.Mapping == nil {
		t.Fatal("expected mapping to be set")
	}
	if template.Mapping.Type != "text" {
		t.Errorf("expected mapping.type=text, got %s", template.Mapping.Type)
	}
	if template.Mapping.Analyzer != "keyword" {
		t.Errorf("expected mapping.analyzer=keyword, got %s", template.Mapping.Analyzer)
	}
}

func TestDynamicTemplateJSONStrict(t *testing.T) {
	MappingJSONStrict = true
	defer func() {
		MappingJSONStrict = false
	}()

	jsonData := `{
		"name": "test",
		"invalid_key": "value"
	}`

	var template DynamicTemplate
	err := json.Unmarshal([]byte(jsonData), &template)
	if err == nil {
		t.Error("expected error for invalid key in strict mode")
	}
}

func TestDocumentMappingWithDynamicTemplates(t *testing.T) {
	jsonData := `{
		"enabled": true,
		"dynamic": true,
		"dynamic_templates": [
			{
				"name": "text_fields",
				"match": "*_text",
				"mapping": {
					"type": "text",
					"analyzer": "en"
				}
			},
			{
				"name": "keyword_fields",
				"match": "*_keyword",
				"mapping": {
					"type": "text",
					"analyzer": "keyword"
				}
			}
		]
	}`

	var dm DocumentMapping
	err := json.Unmarshal([]byte(jsonData), &dm)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(dm.DynamicTemplates) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(dm.DynamicTemplates))
	}

	if dm.DynamicTemplates[0].Name != "text_fields" {
		t.Errorf("expected first template name=text_fields, got %s", dm.DynamicTemplates[0].Name)
	}
	if dm.DynamicTemplates[1].Name != "keyword_fields" {
		t.Errorf("expected second template name=keyword_fields, got %s", dm.DynamicTemplates[1].Name)
	}
}

func TestDynamicTemplateFluentAPI(t *testing.T) {
	template := NewDynamicTemplate("my_template").
		MatchField("*_text").
		UnmatchField("skip_*").
		MatchPath("data.**").
		UnmatchPath("data.internal.**").
		MatchType("string").
		WithMapping(NewTextFieldMapping())

	if template.Name != "my_template" {
		t.Errorf("expected name=my_template, got %s", template.Name)
	}
	if template.Match != "*_text" {
		t.Errorf("expected match=*_text, got %s", template.Match)
	}
	if template.Unmatch != "skip_*" {
		t.Errorf("expected unmatch=skip_*, got %s", template.Unmatch)
	}
	if template.PathMatch != "data.**" {
		t.Errorf("expected path_match=data.**, got %s", template.PathMatch)
	}
	if template.PathUnmatch != "data.internal.**" {
		t.Errorf("expected path_unmatch=data.internal.**, got %s", template.PathUnmatch)
	}
	if template.MatchMappingType != "string" {
		t.Errorf("expected match_mapping_type=string, got %s", template.MatchMappingType)
	}
	if template.Mapping == nil {
		t.Error("expected mapping to be set")
	}
}

func TestAddDynamicTemplate(t *testing.T) {
	dm := NewDocumentMapping()

	template1 := NewDynamicTemplate("template1").MatchField("*_text")
	template2 := NewDynamicTemplate("template2").MatchField("*_keyword")

	dm.AddDynamicTemplate(template1)
	dm.AddDynamicTemplate(template2)

	if len(dm.DynamicTemplates) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(dm.DynamicTemplates))
	}
}

func TestFindMatchingTemplate(t *testing.T) {
	dm := NewDocumentMapping()

	textTemplate := NewDynamicTemplate("text_fields").
		MatchField("*_text").
		WithMapping(&FieldMapping{Type: "text", Analyzer: "en"})

	keywordTemplate := NewDynamicTemplate("keyword_fields").
		MatchField("*_keyword").
		WithMapping(&FieldMapping{Type: "text", Analyzer: "keyword"})

	dm.AddDynamicTemplate(textTemplate)
	dm.AddDynamicTemplate(keywordTemplate)

	// Test finding the text template
	found := dm.findMatchingTemplate("title_text", "title_text", "string", nil)
	if found == nil {
		t.Fatal("expected to find template")
	}
	if found.Name != "text_fields" {
		t.Errorf("expected template name=text_fields, got %s", found.Name)
	}

	// Test finding the keyword template
	found = dm.findMatchingTemplate("status_keyword", "status_keyword", "string", nil)
	if found == nil {
		t.Fatal("expected to find template")
	}
	if found.Name != "keyword_fields" {
		t.Errorf("expected template name=keyword_fields, got %s", found.Name)
	}

	// Test no match
	found = dm.findMatchingTemplate("other_field", "other_field", "string", nil)
	if found != nil {
		t.Error("expected no template match")
	}
}

func TestFindMatchingTemplateWithInheritance(t *testing.T) {
	dm := NewDocumentMapping()

	// Parent template
	parentTemplate := NewDynamicTemplate("parent_strings").
		MatchType("string").
		WithMapping(&FieldMapping{Type: "text", Analyzer: "standard"})
	dm.AddDynamicTemplate(parentTemplate)

	// Child mapping with its own template
	childMapping := NewDocumentMapping()
	childTemplate := NewDynamicTemplate("child_text").
		MatchField("*_text").
		WithMapping(&FieldMapping{Type: "text", Analyzer: "en"})
	childMapping.AddDynamicTemplate(childTemplate)
	dm.AddSubDocumentMapping("child", childMapping)

	// Test that child template takes precedence for matching fields
	parentTemplates := dm.DynamicTemplates
	found := childMapping.findMatchingTemplate("title_text", "child.title_text", "string", parentTemplates)
	if found == nil {
		t.Fatal("expected to find template")
	}
	if found.Name != "child_text" {
		t.Errorf("expected child template to match, got %s", found.Name)
	}

	// Test that parent template is used when child doesn't match
	found = childMapping.findMatchingTemplate("other_field", "child.other_field", "string", parentTemplates)
	if found == nil {
		t.Fatal("expected to find parent template")
	}
	if found.Name != "parent_strings" {
		t.Errorf("expected parent template to match, got %s", found.Name)
	}
}

func TestCollectTemplatesAlongPath(t *testing.T) {
	dm := NewDocumentMapping()
	dm.AddDynamicTemplate(NewDynamicTemplate("root"))

	level1 := NewDocumentMapping()
	level1.AddDynamicTemplate(NewDynamicTemplate("level1"))
	dm.AddSubDocumentMapping("level1", level1)

	level2 := NewDocumentMapping()
	level2.AddDynamicTemplate(NewDynamicTemplate("level2"))
	level1.AddSubDocumentMapping("level2", level2)

	// Collect templates for a path at level 3
	templates := dm.collectTemplatesAlongPath([]string{"level1", "level2", "field"})

	// Should have templates from root and level1 (not level2 since field is at level2)
	if len(templates) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(templates))
	}

	names := make([]string, len(templates))
	for i, tmpl := range templates {
		names[i] = tmpl.Name
	}

	// Check that root template is first (inheritance order)
	if templates[0].Name != "root" {
		t.Errorf("expected first template to be 'root', got %s", templates[0].Name)
	}
	if templates[1].Name != "level1" {
		t.Errorf("expected second template to be 'level1', got %s", templates[1].Name)
	}
}

func TestDynamicTemplateWithDocumentMapping(t *testing.T) {
	mapping := NewIndexMapping()

	// Add a dynamic template that matches all string fields ending in _keyword
	// and uses the keyword analyzer
	keywordTemplate := NewDynamicTemplate("keyword_strings").
		MatchField("*_keyword").
		MatchType("string").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "keyword",
			Store:    true,
			Index:    true,
		})

	mapping.DefaultMapping.AddDynamicTemplate(keywordTemplate)

	// Create a document with a field that matches the template
	data := map[string]interface{}{
		"name":           "test",
		"status_keyword": "active",
	}

	doc := document.NewDocument("1")
	err := mapping.MapDocument(doc, data)
	if err != nil {
		t.Fatalf("failed to map document: %v", err)
	}

	// Verify the fields were created
	foundName := false
	foundStatus := false
	for _, field := range doc.Fields {
		switch field.Name() {
		case "name":
			foundName = true
		case "status_keyword":
			foundStatus = true
		}
	}

	if !foundName {
		t.Error("expected to find 'name' field")
	}
	if !foundStatus {
		t.Error("expected to find 'status_keyword' field")
	}
}

func TestDynamicTemplateWithNumbers(t *testing.T) {
	mapping := NewIndexMapping()

	// Add a template that matches number fields ending in _count
	// and disables doc values
	countTemplate := NewDynamicTemplate("count_fields").
		MatchField("*_count").
		MatchType("number").
		WithMapping(&FieldMapping{
			Type:      "number",
			Store:     true,
			Index:     true,
			DocValues: false,
		})

	mapping.DefaultMapping.AddDynamicTemplate(countTemplate)

	data := map[string]interface{}{
		"item_count": 42,
		"total":      100,
	}

	doc := document.NewDocument("1")
	err := mapping.MapDocument(doc, data)
	if err != nil {
		t.Fatalf("failed to map document: %v", err)
	}

	foundItemCount := false
	foundTotal := false
	for _, field := range doc.Fields {
		switch field.Name() {
		case "item_count":
			foundItemCount = true
		case "total":
			foundTotal = true
		}
	}

	if !foundItemCount {
		t.Error("expected to find 'item_count' field")
	}
	if !foundTotal {
		t.Error("expected to find 'total' field")
	}
}

func TestDynamicTemplateWithBooleans(t *testing.T) {
	mapping := NewIndexMapping()

	// Add a template that matches boolean fields ending in _flag
	flagTemplate := NewDynamicTemplate("flag_fields").
		MatchField("*_flag").
		MatchType("boolean").
		WithMapping(&FieldMapping{
			Type:  "boolean",
			Store: true,
			Index: true,
		})

	mapping.DefaultMapping.AddDynamicTemplate(flagTemplate)

	data := map[string]interface{}{
		"active_flag": true,
		"enabled":     false,
	}

	doc := document.NewDocument("1")
	err := mapping.MapDocument(doc, data)
	if err != nil {
		t.Fatalf("failed to map document: %v", err)
	}

	foundActiveFlag := false
	foundEnabled := false
	for _, field := range doc.Fields {
		switch field.Name() {
		case "active_flag":
			foundActiveFlag = true
		case "enabled":
			foundEnabled = true
		}
	}

	if !foundActiveFlag {
		t.Error("expected to find 'active_flag' field")
	}
	if !foundEnabled {
		t.Error("expected to find 'enabled' field")
	}
}

func TestDynamicTemplateWithPathMatch(t *testing.T) {
	mapping := NewIndexMapping()

	// Add a template that matches all fields under metadata.**
	metadataTemplate := NewDynamicTemplate("metadata_fields").
		MatchPath("metadata.**").
		MatchType("string").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "keyword",
			Store:    true,
			Index:    true,
		})

	mapping.DefaultMapping.AddDynamicTemplate(metadataTemplate)

	data := map[string]interface{}{
		"title": "Test Document",
		"metadata": map[string]interface{}{
			"author": "John Doe",
			"tags": map[string]interface{}{
				"primary": "test",
			},
		},
	}

	doc := document.NewDocument("1")
	err := mapping.MapDocument(doc, data)
	if err != nil {
		t.Fatalf("failed to map document: %v", err)
	}

	foundTitle := false
	foundAuthor := false
	foundPrimary := false
	for _, field := range doc.Fields {
		switch field.Name() {
		case "title":
			foundTitle = true
		case "metadata.author":
			foundAuthor = true
		case "metadata.tags.primary":
			foundPrimary = true
		}
	}

	if !foundTitle {
		t.Error("expected to find 'title' field")
	}
	if !foundAuthor {
		t.Error("expected to find 'metadata.author' field")
	}
	if !foundPrimary {
		t.Error("expected to find 'metadata.tags.primary' field")
	}
}

func TestDynamicTemplateWithDateTime(t *testing.T) {
	mapping := NewIndexMapping()

	// Add a template that matches datetime fields ending in _at
	dateTemplate := NewDynamicTemplate("datetime_fields").
		MatchField("*_at").
		MatchType("date").
		WithMapping(&FieldMapping{
			Type:  "datetime",
			Store: true,
			Index: true,
		})

	mapping.DefaultMapping.AddDynamicTemplate(dateTemplate)

	now := time.Now()
	data := map[string]interface{}{
		"created_at": now,
		"timestamp":  now,
	}

	doc := document.NewDocument("1")
	err := mapping.MapDocument(doc, data)
	if err != nil {
		t.Fatalf("failed to map document: %v", err)
	}

	foundCreatedAt := false
	foundTimestamp := false
	for _, field := range doc.Fields {
		switch field.Name() {
		case "created_at":
			foundCreatedAt = true
		case "timestamp":
			foundTimestamp = true
		}
	}

	if !foundCreatedAt {
		t.Error("expected to find 'created_at' field")
	}
	if !foundTimestamp {
		t.Error("expected to find 'timestamp' field")
	}
}

func TestDynamicTemplateOrder(t *testing.T) {
	// Test that templates are evaluated in order and first match wins
	mapping := NewIndexMapping()

	// First template - more specific
	specificTemplate := NewDynamicTemplate("specific").
		MatchField("special_*").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "keyword",
			Store:    true,
			Index:    true,
		})

	// Second template - catches all strings
	catchAllTemplate := NewDynamicTemplate("catch_all").
		MatchType("string").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "standard",
			Store:    true,
			Index:    true,
		})

	mapping.DefaultMapping.AddDynamicTemplate(specificTemplate)
	mapping.DefaultMapping.AddDynamicTemplate(catchAllTemplate)

	// Test that special_field matches the first template
	found := mapping.DefaultMapping.findMatchingTemplate("special_field", "special_field", "string", nil)
	if found == nil || found.Name != "specific" {
		t.Error("expected 'specific' template to match first")
	}

	// Test that other_field matches the second template
	found = mapping.DefaultMapping.findMatchingTemplate("other_field", "other_field", "string", nil)
	if found == nil || found.Name != "catch_all" {
		t.Error("expected 'catch_all' template to match")
	}
}

func TestDynamicTemplateInheritanceOverride(t *testing.T) {
	mapping := NewIndexMapping()

	// Parent template for all strings
	parentTemplate := NewDynamicTemplate("parent_strings").
		MatchType("string").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "standard",
			Store:    true,
			Index:    true,
		})
	mapping.DefaultMapping.AddDynamicTemplate(parentTemplate)

	// Create a child mapping that overrides for *_keyword fields
	childMapping := NewDocumentMapping()
	childTemplate := NewDynamicTemplate("child_keyword").
		MatchField("*_keyword").
		WithMapping(&FieldMapping{
			Type:     "text",
			Analyzer: "keyword",
			Store:    true,
			Index:    true,
		})
	childMapping.AddDynamicTemplate(childTemplate)
	mapping.DefaultMapping.AddSubDocumentMapping("nested", childMapping)

	// Collect parent templates
	parentTemplates := mapping.DefaultMapping.DynamicTemplates

	// Test that child template takes precedence for *_keyword
	found := childMapping.findMatchingTemplate("status_keyword", "nested.status_keyword", "string", parentTemplates)
	if found == nil || found.Name != "child_keyword" {
		t.Error("expected 'child_keyword' template to match")
	}

	// Test that parent template is used for other strings
	found = childMapping.findMatchingTemplate("title", "nested.title", "string", parentTemplates)
	if found == nil || found.Name != "parent_strings" {
		t.Error("expected 'parent_strings' template to match")
	}
}
