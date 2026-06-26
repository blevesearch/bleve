package bleve

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search/query"
)

func TestIndexV2(t *testing.T) {

	indexPath := "/Users/likithb/Desktop/Code/MB-72376/indexes/idx1"
	_, err := os.Stat(indexPath)
	if err == nil {
		err = os.RemoveAll(indexPath)
		if err != nil {
			panic(err)
		}
	}

	indexMapping := NewIndexMapping()
	docMapping := NewDocumentStaticMapping()
	docMapping.Dynamic = false
	geoField := NewGeoShapeV2FieldMapping()
	docMapping.AddFieldMappingsAt("geog", geoField)
	indexMapping.DefaultMapping = docMapping
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	indexMapping.DocValuesDynamic = false

	index, err := New(indexPath, indexMapping)
	if err != nil {
		panic(err)
	}
	defer index.Close()

	records := testRecords(t)

	docNum := 0
	batchSize := 1000
	batch := index.NewBatch()

	for _, record := range records {
		doc := make(map[string]interface{}, 1)
		doc["geog"] = record.Geog

		id := fmt.Sprintf("%d", record.InventoryID)
		err := batch.Index(id, doc)
		if err != nil {
			panic(err)
		}

		docNum++

		if docNum%batchSize == 0 {
			err = index.Batch(batch)
			if err != nil {
				panic(err)
			}
			batch = index.NewBatch()
			fmt.Printf("Indexed %d documents\n", docNum)
		}
	}

	if batch.Size() > 0 {
		err = index.Batch(batch)
		if err != nil {
			panic(err)
		}
	}
}

func TestQueryV2(t *testing.T) {
	indexPath := "/Users/likithb/Desktop/Code/MB-72376/indexes/idx1"
	index, err := Open(indexPath)
	if err != nil {
		panic(err)
	}
	defer index.Close()

	geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,59],[-147,59],[-147,62],[-152,62]]]}` // #1 time 774 ms, hits: 51,080

	queryShape, err := geo.ParseGeoJSONShape([]byte(geojson))
	if err != nil {
		panic(err)
	}

	query := &query.GeoShapeV2Query{
		FieldVal: "geog",
		GeometryV2: query.Geometry{
			Shape:    queryShape,
			Relation: "contains",
		},
	}

	avgTime := 0
	for i := 0; i < 100; i++ {

		sreq := NewSearchRequest(query)
		sreq.Size = 10
		sreq.Fields = []string{"geog"}

		sres, err := index.Search(sreq)
		if err != nil {
			panic(err)
		}

		docCount, err := index.DocCount()
		if err != nil {
			panic(err)
		}
		avgTime += int(sres.Took.Microseconds())
		fmt.Println("Document count:", docCount)
		fmt.Printf("Took: %s\n", sres.Took.String())
		fmt.Printf("Total hits: %d\n", sres.Total)
	}

	fmt.Printf("Average time (ms): %f\n", float64(avgTime)/1000/100)
}

type testRecord struct {
	InventoryID int64       `json:"inventory_id"`
	Geog        interface{} `json:"geog"`
}

func testRecords(t *testing.T) []testRecord {
	file, err := os.Open("/Users/likithb/Desktop/Code/gitIssues/2221/simple-bleve-search/output.brotli")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	brotliReader := brotli.NewReader(file)

	var records []testRecord
	err = json.NewDecoder(brotliReader).Decode(&records)
	if err != nil {
		panic(err)
	}
	return records
}

func BenchmarkQueryV2(b *testing.B) {
	indexPath := "/Users/likithb/Desktop/Code/MB-72376/indexes/idx1"
	index, err := Open(indexPath)
	if err != nil {
		b.Fatal(err)
	}
	defer index.Close()

	geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,59],[-147,59],[-147,62],[-152,62]]]}`
	queryShape, err := geo.ParseGeoJSONShape([]byte(geojson))
	if err != nil {
		b.Fatal(err)
	}

	q := &query.GeoShapeV2Query{
		FieldVal: "geog",
		GeometryV2: query.Geometry{
			Shape:    queryShape,
			Relation: "contains",
		},
	}

	sreq := NewSearchRequest(q)
	sreq.Size = 10
	sreq.Fields = []string{"geog"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := index.Search(sreq); err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndex(t *testing.T) {

	indexPath := "/Users/likithb/Desktop/Code/MB-72376/indexes/idx2"
	_, err := os.Stat(indexPath)
	if err == nil {
		err = os.RemoveAll(indexPath)
		if err != nil {
			panic(err)
		}
	}

	indexMapping := NewIndexMapping()
	docMapping := NewDocumentStaticMapping()
	geoField := NewGeoShapeFieldMapping()
	docMapping.AddFieldMappingsAt("geog", geoField)
	indexMapping.DefaultMapping = docMapping

	index, err := New(indexPath, indexMapping)
	if err != nil {
		panic(err)
	}
	defer index.Close()

	records := testRecords(t)

	docNum := 0
	batchSize := 1000
	batch := index.NewBatch()

	for _, record := range records {
		doc := make(map[string]interface{}, 1)
		doc["geog"] = record.Geog

		id := fmt.Sprintf("%d", record.InventoryID)
		err := batch.Index(id, doc)
		if err != nil {
			panic(err)
		}

		docNum++

		if docNum%batchSize == 0 {
			err = index.Batch(batch)
			if err != nil {
				panic(err)
			}
			batch = index.NewBatch()
			fmt.Printf("Indexed %d documents\n", docNum)
		}
	}

	if batch.Size() > 0 {
		err = index.Batch(batch)
		if err != nil {
			panic(err)
		}
	}
}

func TestQuery(t *testing.T) {
	indexPath := "/Users/likithb/Desktop/Code/MB-72376/indexes/idx2"
	index, err := Open(indexPath)
	if err != nil {
		panic(err)
	}
	defer index.Close()

	geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,59],[-147,59],[-147,62],[-152,62]]]}` // #1 time 774 ms, hits: 51,080

	queryShape, err := geo.ParseGeoJSONShape([]byte(geojson))
	if err != nil {
		panic(err)
	}

	query := &query.GeoShapeQuery{
		FieldVal: "geog",
		Geometry: query.Geometry{
			Shape:    queryShape,
			Relation: "within",
		},
	}

	avgTime := 0
	for i := 0; i < 100; i++ {

		sreq := NewSearchRequest(query)
		sreq.Size = 10
		sreq.Fields = []string{"geog"}

		sres, err := index.Search(sreq)
		if err != nil {
			panic(err)
		}

		docCount, err := index.DocCount()
		if err != nil {
			panic(err)
		}
		avgTime += int(sres.Took.Microseconds())
		fmt.Println("Document count:", docCount)
		fmt.Printf("Took: %s\n", sres.Took.String())
		fmt.Printf("Total hits: %d\n", sres.Total)
	}

	fmt.Printf("Average time (ms): %f\n", float64(avgTime)/1000/100)
}
