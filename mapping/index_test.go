package mapping

import (
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
)

func TestValidate(t *testing.T) {
	im := createTestIndexMappingImpl()
	err := im.Validate()
	if err != nil {
		t.Fatalf("unexpected error while validating index mapping: %v", err)
	}
}

func createTestIndexMappingImpl() *IndexMappingImpl {
	textFieldMapping := NewTextFieldMapping()
	textFieldMapping.Analyzer = en.AnalyzerName

	keywordMapping := NewTextFieldMapping()
	keywordMapping.Analyzer = keyword.Name

	indexMapping := NewIndexMapping()
	first := buildFirstChildTestMapping(textFieldMapping, keywordMapping)
	indexMapping.AddDocumentMapping("child", first)
	second := buildSecondChildTestMapping(textFieldMapping, keywordMapping, first)
	indexMapping.AddDocumentMapping("child", second)

	return indexMapping
}

func buildFirstChildTestMapping(textFieldMapping, keywordMapping *FieldMapping) *DocumentMapping {
	m := NewDocumentMapping()
	m.AddFieldMappingsAt("name", keywordMapping)
	m.AddFieldMappingsAt("description", textFieldMapping)

	return m
}

func buildSecondChildTestMapping(textFieldMapping, keywordMapping *FieldMapping, sibling *DocumentMapping) *DocumentMapping {
	m := NewDocumentMapping()
	m.AddFieldMappingsAt("name", keywordMapping)
	m.AddFieldMappingsAt("description", textFieldMapping)
	m.AddSubDocumentMapping("first", sibling)

	return m
}
