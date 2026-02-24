//go:build vectors
// +build vectors

package bleve

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

func loadSiftData() ([]map[string]interface{}, error) {
	fileContent, err := os.ReadFile("~/fts/data/datasets/vec-sift-bucket.json")
	if err != nil {
		return nil, err
	}
	var documents []map[string]interface{}
	err = json.Unmarshal(fileContent, &documents)
	if err != nil {
		return nil, err
	}
	return documents, nil
}

func TestCentroidIndex(t *testing.T) {
	_, _, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents, err := loadSiftData()
	if err != nil {
		t.Fatal(err)
	}
	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = en.AnalyzerName

	vecFieldMappingL2 := mapping.NewVectorFieldMapping()
	vecFieldMappingL2.Dims = 128
	vecFieldMappingL2.Similarity = index.EuclideanDistance

	indexMappingL2Norm := NewIndexMapping()
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingL2)

	idx, err := newIndexUsing(t.TempDir(), indexMappingL2Norm, Config.DefaultIndexType, Config.DefaultKVStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := idx.NewBatch()
	for _, doc := range documents[:100000] {
		docId := fmt.Sprintf("%s:%s", index.TrainDataPrefix, doc["id"])
		err = batch.Index(docId, doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = idx.Train(batch)
	if err != nil {
		t.Fatal(err)
	}
}
