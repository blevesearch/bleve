package test

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

type doc struct {
	IP string `json:"ip"`
}

func createIdx(t *testing.T) bleve.Index {
	ipIndexed := mapping.NewIPFieldMapping()
	ipIndexed.Name = "ip"

	lineMapping := bleve.NewDocumentStaticMapping()
	lineMapping.AddFieldMappingsAt("ip", ipIndexed)

	mapping := bleve.NewIndexMapping()
	mapping.DefaultMapping = lineMapping
	mapping.DefaultAnalyzer = "standard"

	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func Test_ipv4CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.21"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_ipv6CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"2a00:23c8:7283:ff00:1fa8:2af6:9dec:6b19"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `2a00:23c8:7283:ff00:1fa8:0:0:0/80`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_MultiIpvr4CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id2", doc{"192.168.1.255"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id3", doc{"192.168.2.22"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 2 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].ID)
	}
	if res.Hits[1].ID != "id2" {
		t.Fatalf("expected %q got %q", "id2", res.Hits[0].Index)
	}
}

func Test_CidrQueryNonDivisibleBy8(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.01"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id2", doc{"192.168.1.02"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id3", doc{"192.168.2.5"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id4", doc{"192.168.2.6"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/30`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 2 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].ID)
	}
	if res.Hits[1].ID != "id2" {
		t.Fatalf("expected %q got %q", "id2", res.Hits[0].Index)
	}
}

func Test_simpleIpv4MatchQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.21"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.21`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}
