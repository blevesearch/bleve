package bleve

import (
	"testing"
)

func TestSynonymInMem(t *testing.T) {
	doc := struct {
		Text string `json:"text"`
	}{
		Text: "hardworking employee",
	}

	synDef := &SynonymDefinition{
		Synonyms: []string{"hardworking", "industrious", "conscientious", "persistent"},
	}

	bleveMapping := NewIndexMapping()
	err := bleveMapping.AddSynonymSource("english", map[string]interface{}{
		"collection": "collection1",
		"analyzer":   "en",
	})
	if err != nil {
		t.Fatal(err)
	}

	textFieldMapping := NewTextFieldMapping()
	textFieldMapping.Analyzer = "en"
	textFieldMapping.SynonymSource = "english"
	bleveMapping.DefaultMapping.AddFieldMappingsAt("text", textFieldMapping)

	// HERE IS THE FAILURE: Using NewMemOnly instead of New()
	index, err := NewMemOnly(bleveMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	err = index.Index("doc1", doc)
	if err != nil {
		t.Fatal(err)
	}

	if synIndex, ok := index.(SynonymIndex); ok {
		err = synIndex.IndexSynonym("synDoc1", "collection1", synDef)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("expected synonym index support")
	}

	query := NewMatchQuery("persistent")
	query.SetField("text")
	searchRequest := NewSearchRequest(query)
	searchResult, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	if searchResult.Total != 1 {
		t.Errorf("Expected 1 match, got %d", searchResult.Total)
	}
}
