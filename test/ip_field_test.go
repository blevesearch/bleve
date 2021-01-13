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

	err = idx.Index("id1", doc{"192.168.1.21", 123.0})
	if err != nil {
		t.Fatal(err)
	}

	n, err := idx.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("failed to insert doc")
	}
	doc1, err := idx.Document("id1")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(doc1)

	fd, err := idx.FieldDict("ip")
	if err != nil {
		t.Fatal(err)
	}
	e, err := fd.Next()
	if err != nil {
		t.Fatal(err)
	}

	t.Log([]byte(e.Term), e.Count)
	if e.Term != string([]byte{192, 168, 1, 21}) {
		t.Fatal("expected to find ip 192.168.1.21")
	}
	fd.Close()

	pd, err := idx.FieldDictPrefix("ip", []byte{192, 168, 1})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := pd.Next()
	if err != nil {
		t.Fatal(err)
	}
	if e2.Term != string([]byte{192, 168, 1, 21}) {
		t.Fatal("expected to find ip 192.168.1.21")
	}
	pd.Close()

	min := 120.0
	max := 130.0
	q1 := bleve.NewNumericRangeQuery(&min, &max)
	q1.FieldVal = "num"

	search := bleve.NewSearchRequest(q1)
	search.Fields = []string{"*"}
	search.Explain = true
	search.IncludeLocations = true
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(res)
	if res.Total != 1 {
		t.Fatalf("failed to find Num, res -> %s", res)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search = bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	search.Explain = true
	search.IncludeLocations = true
	res, err = idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	// TODO this fails
	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}

}
