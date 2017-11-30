package scorch

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

func TestIndexReader(t *testing.T) {
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{}, []byte("test test test"), testAnalyzer))
	doc.AddField(document.NewTextFieldCustom("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors, testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// first look for a term that doesn't exist
	reader, err := indexReader.TermFieldReader([]byte("nope"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count := reader.Count()
	if count != 0 {
		t.Errorf("Expected doc count to be: %d got: %d", 0, count)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader, err = indexReader.TermFieldReader([]byte("test"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	expectedCount = 2
	count = reader.Count()
	if count != expectedCount {
		t.Errorf("Exptected doc count to be: %d got: %d", expectedCount, count)
	}

	var match *index.TermFieldDoc
	var actualCount uint64
	match, err = reader.Next(nil)
	for err == nil && match != nil {
		match, err = reader.Next(nil)
		if err != nil {
			t.Errorf("unexpected error reading next")
		}
		actualCount++
	}
	if actualCount != count {
		t.Errorf("count was 2, but only saw %d", actualCount)
	}

	internalIDBogus, err := indexReader.InternalID("a-bogus-docId")
	if err != nil {
		t.Fatal(err)
	}
	if internalIDBogus != nil {
		t.Errorf("expected bogus docId to have nil InternalID")
	}

	internalID2, err := indexReader.InternalID("2")
	if err != nil {
		t.Fatal(err)
	}
	expectedMatch := &index.TermFieldDoc{
		ID:   internalID2,
		Freq: 1,
		Norm: 0.5773502588272095,
		Vectors: []*index.TermFieldVector{
			{
				Field: "desc",
				Pos:   3,
				Start: 9,
				End:   13,
			},
		},
	}
	tfr, err := indexReader.TermFieldReader([]byte("rice"), "desc", true, true, true)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	match, err = tfr.Next(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expectedMatch, match) {
		t.Errorf("got %#v, expected %#v", match, expectedMatch)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now test usage of advance
	reader, err = indexReader.TermFieldReader([]byte("test"), "name", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}

	match, err = reader.Advance(internalID2, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match == nil {
		t.Fatalf("Expected match, got nil")
	}
	if !match.ID.Equals(internalID2) {
		t.Errorf("Expected ID '2', got '%s'", match.ID)
	}
	// NOTE: no point in changing this to internal id 3, there is no id 3
	// the test is looking for something that doens't exist and this doesn't
	match, err = reader.Advance(index.IndexInternalID("3"), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now test creating a reader for a field that doesn't exist
	reader, err = indexReader.TermFieldReader([]byte("water"), "doesnotexist", true, true, true)
	if err != nil {
		t.Errorf("Error accessing term field reader: %v", err)
	}
	count = reader.Count()
	if count != 0 {
		t.Errorf("expected count 0 for reader of non-existent field")
	}
	match, err = reader.Next(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}
	match, err = reader.Advance(index.IndexInternalID("anywhere"), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match != nil {
		t.Errorf("expected nil, got %v", match)
	}

}

func TestIndexDocIdReader(t *testing.T) {
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var expectedCount uint64
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test test test")))
	doc.AddField(document.NewTextFieldWithIndexingOptions("desc", []uint64{}, []byte("eat more rice"), document.IndexField|document.IncludeTermVectors))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	// first get all doc ids
	reader, err := indexReader.DocIDReaderAll()
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := reader.Next()
	count := uint64(0)
	for id != nil {
		count++
		id, err = reader.Next()
	}
	if count != expectedCount {
		t.Errorf("expected %d, got %d", expectedCount, count)
	}

	// try it again, but jump to the second doc this time
	reader2, err := indexReader.DocIDReaderAll()
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader2.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	internalID2, err := indexReader.InternalID("2")
	if err != nil {
		t.Fatal(err)
	}

	id, err = reader2.Advance(internalID2)
	if err != nil {
		t.Error(err)
	}
	if !id.Equals(internalID2) {
		t.Errorf("expected to find id '2', got '%s'", id)
	}

	// again 3 doesn't exist cannot use internal id for 3 as there is none
	// the important aspect is that this id doesn't exist, so its ok
	id, err = reader2.Advance(index.IndexInternalID("3"))
	if err != nil {
		t.Error(err)
	}
	if id != nil {
		t.Errorf("expected to find id '', got '%s'", id)
	}
}

func TestIndexDocIdOnlyReader(t *testing.T) {
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := document.NewDocument("1")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("3")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("5")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("7")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	doc = document.NewDocument("9")
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	onlyIds := []string{"1", "5", "9"}
	reader, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	id, err := reader.Next()
	count := uint64(0)
	for id != nil {
		count++
		id, err = reader.Next()
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}

	// commented out because advance works with internal ids
	// this test presumes we see items in external doc id order
	// which is no longer the case, so simply converting external ids
	// to internal ones is not logically correct
	// not removing though because we need some way to test Advance()

	// // try it again, but jump
	// reader2, err := indexReader.DocIDReaderOnly(onlyIds)
	// if err != nil {
	// 	t.Errorf("Error accessing doc id reader: %v", err)
	// }
	// defer func() {
	// 	err := reader2.Close()
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// }()
	//
	// id, err = reader2.Advance(index.IndexInternalID("5"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("5")) {
	// 	t.Errorf("expected to find id '5', got '%s'", id)
	// }
	//
	// id, err = reader2.Advance(index.IndexInternalID("a"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if id != nil {
	// 	t.Errorf("expected to find id '', got '%s'", id)
	// }

	// some keys aren't actually there
	onlyIds = []string{"0", "2", "4", "5", "6", "8", "a"}
	reader3, err := indexReader.DocIDReaderOnly(onlyIds)
	if err != nil {
		t.Errorf("Error accessing doc id reader: %v", err)
	}
	defer func() {
		err := reader3.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	id, err = reader3.Next()
	count = uint64(0)
	for id != nil {
		count++
		id, err = reader3.Next()
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}

	// commented out because advance works with internal ids
	// this test presumes we see items in external doc id order
	// which is no longer the case, so simply converting external ids
	// to internal ones is not logically correct
	// not removing though because we need some way to test Advance()

	// // mix advance and next
	// onlyIds = []string{"0", "1", "3", "5", "6", "9"}
	// reader4, err := indexReader.DocIDReaderOnly(onlyIds)
	// if err != nil {
	// 	t.Errorf("Error accessing doc id reader: %v", err)
	// }
	// defer func() {
	// 	err := reader4.Close()
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// }()
	//
	// // first key is "1"
	// id, err = reader4.Next()
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("1")) {
	// 	t.Errorf("expected to find id '1', got '%s'", id)
	// }
	//
	// // advancing to key we dont have gives next
	// id, err = reader4.Advance(index.IndexInternalID("2"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("3")) {
	// 	t.Errorf("expected to find id '3', got '%s'", id)
	// }
	//
	// // next after advance works
	// id, err = reader4.Next()
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("5")) {
	// 	t.Errorf("expected to find id '5', got '%s'", id)
	// }
	//
	// // advancing to key we do have works
	// id, err = reader4.Advance(index.IndexInternalID("9"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("9")) {
	// 	t.Errorf("expected to find id '9', got '%s'", id)
	// }
	//
	// // advance backwards at end
	// id, err = reader4.Advance(index.IndexInternalID("4"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("5")) {
	// 	t.Errorf("expected to find id '5', got '%s'", id)
	// }
	//
	// // next after advance works
	// id, err = reader4.Next()
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("9")) {
	// 	t.Errorf("expected to find id '9', got '%s'", id)
	// }
	//
	// // advance backwards to key that exists, but not in only set
	// id, err = reader4.Advance(index.IndexInternalID("7"))
	// if err != nil {
	// 	t.Error(err)
	// }
	// if !id.Equals(index.IndexInternalID("9")) {
	// 	t.Errorf("expected to find id '9', got '%s'", id)
	// }

}
