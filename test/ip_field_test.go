package test

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

type doc struct {
	IP  string `json:"ip"`
	Num int    `json:"num"`
}

func Test_iprange(t *testing.T) {
	ipIndexed := mapping.NewIPFieldMapping()
	ipIndexed.Name = "ip"

	numIndex := mapping.NewNumericFieldMapping()

	lineMapping := bleve.NewDocumentStaticMapping()
	lineMapping.AddFieldMappingsAt("ip", ipIndexed)
	lineMapping.AddFieldMappingsAt("num", numIndex)

	mapping := bleve.NewIndexMapping()
	mapping.DefaultMapping = lineMapping
	mapping.DefaultAnalyzer = "standard"

	idx, err := bleve.New(t.TempDir(), mapping)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	err = idx.Index("id1", doc{"192.168.1.21", 123})
	if err != nil {
		t.Fatal(err)
	}

	n, err := idx.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if n !=1 {
		t.Fatal("failed to insert doc")
	}
	doc1, err  := idx.Document("id1")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(doc1)

	doc2, err  := idx.Document("id2")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(doc2)

	query := bleve.NewIPRangeQuery(`192.200.1.21`)
	query.FieldVal = "ip"

	//query := bleve.NewMatchAllQuery()
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	search.Explain = true
	search.IncludeLocations = true
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(res)
	if res.Total != 1 {
		t.Fatalf("failed to find ip, res -> %s", res)
	}

}
