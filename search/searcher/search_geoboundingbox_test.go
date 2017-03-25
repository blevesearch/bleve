package searcher

import (
	"log"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/search"
)

func TestGeoBoundingBox(t *testing.T) {
	i := setup(t)
	indexReader, err := i.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	gbs, err := NewGeoBoundingBoxSearcher(indexReader, 0.001, 0.001, 0.002, 0.002, "loc", 1.0, search.SearcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(gbs.DocumentMatchPoolSize(), 0),
	}
	docMatch, err := gbs.Next(ctx)
	for docMatch != nil && err == nil {
		if docMatch == nil {
			log.Printf("nil docmatch")
		} else {
			log.Printf("got doc match: %s", docMatch.IndexInternalID)
		}
		docMatch, err = gbs.Next(ctx)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T) index.Index {

	analysisQueue := index.NewAnalysisQueue(1)
	i, err := upsidedown.NewUpsideDownCouch(
		gtreap.Name,
		map[string]interface{}{
			"path": "",
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "a",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 0.0015, 0.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "b",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 1.0015, 1.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "c",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 2.0015, 2.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "d",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 3.0015, 3.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "e",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 4.0015, 4.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "f",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 5.0015, 5.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "g",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 6.0015, 6.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "h",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 7.0015, 7.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "i",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 8.0015, 8.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "j",
		Fields: []document.Field{
			document.NewGeoPointField("loc", []uint64{}, 9.0015, 9.0015),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return i
}
