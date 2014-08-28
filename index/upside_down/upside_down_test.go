//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package upside_down

import (
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/tokenizers/regexp_tokenizer"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/boltdb"
)

var testAnalyzer = &analysis.Analyzer{
	Tokenizer: regexp_tokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
}

func TestIndexOpenReopen(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// opening database should have inserted version
	expectedLength := uint64(1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// now close it
	idx.Close()

	store, err = boltdb.Open("test", "bleve")
	if err != nil {
		t.Fatalf("error opening store: %v", err)
	}
	idx = NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	// now close it
	idx.Close()
}

func TestIndexInsert(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 4 rows (1 for version, 1 for schema field, and 1 for single term, and 1 for the term count,  and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 1 + 1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertThenDelete(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	err = idx.Delete("1")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount -= 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	err = idx.Delete("2")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount -= 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 2 row (1 for version, 1 for schema field)
	expectedLength := uint64(1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertThenUpdate(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// this update should overwrite one term, and introduce one new one
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{}, []byte("test fail"), testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// should have 2 row (1 for version, 1 for schema field, and 2 for the two term, and 2 for the term counts, and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 2 + 2 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// now do another update that should remove one of term
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("fail")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// should have 2 row (1 for version, 1 for schema field, and 1 for the remaining term, and 1 for the term count, and 1 for the back index entry)
	expectedLength = uint64(1 + 1 + 1 + 1 + 1)
	rowCount = idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64 = 0

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// should have 4 rows (1 for version, 1 for schema field, and 2 for single term, and 1 for the term count,  and 2 for the back index entries)
	expectedLength := uint64(1 + 1 + 2 + 1 + 2)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// close and reopen and and one more to testing counting works correctly
	idx.Close()
	store, err = boltdb.Open("test", "bleve")
	idx = NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("expected doc count: %d, got %d", expectedCount, docCount)
	}
}

func TestIndexInsertWithStore(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 6 rows (1 for version, 1 for schema field, and 1 for single term, and 1 for the stored field and 1 for the term count,  and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 1 + 1 + 1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	storedDoc, err := idx.Document("1")
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 1 {
		t.Errorf("expected 1 stored field, got %d", len(storedDoc.Fields))
	}
	textField, ok := storedDoc.Fields[0].(*document.TextField)
	if !ok {
		t.Errorf("expected text field")
	}
	if string(textField.Value()) != "test" {
		t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
	}
}

func TestIndexInternalCRUD(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	// get something that doesnt exist yet
	val, err := idx.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %s", val)
	}

	// set
	err = idx.SetInternal([]byte("key"), []byte("abc"))
	if err != nil {
		t.Error(err)
	}

	// get
	val, err = idx.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "abc" {
		t.Errorf("expected %s, got '%s'", "abc", val)
	}

	// delete
	err = idx.DeleteInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}

	// get again
	val, err = idx.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %s", val)
	}
}

func TestIndexBatch(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0

	// first create 2 docs the old fashioned way
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	// now create a batch which does 3 things
	// insert new doc
	// update existing doc
	// delete existing doc
	// net document count change 0

	batch := make(index.Batch, 0)
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch["3"] = doc
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2updated")))
	batch["2"] = doc
	batch["1"] = nil

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	docIdReader, err := idx.DocIdReader("", "")
	if err != nil {
		t.Error(err)
	}
	docIds := make([]string, 0)
	docId, err := docIdReader.Next()
	for docId != "" && err == nil {
		docIds = append(docIds, docId)
		docId, err = docIdReader.Next()
	}
	if err != nil {
		t.Error(err)
	}
	expectedDocIds := []string{"2", "3"}
	if !reflect.DeepEqual(docIds, expectedDocIds) {
		t.Errorf("expected ids: %v, got ids: %v", expectedDocIds, docIds)
	}
}

func TestIndexInsertUpdateDeleteWithMultipleTypesStored(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, document.INDEX_FIELD|document.STORE_FIELD))
	df, err := document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), document.INDEX_FIELD|document.STORE_FIELD)
	if err != nil {
		t.Error(err)
	}
	doc.AddField(df)
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 72 rows
	// 1 for version
	// 3 for schema fields
	// 1 for text term
	// 16 for numeric terms
	// 16 for date terms
	// 3 for the stored field
	// 1 for the text term count
	// 16 for numeric term counts
	// 16 for date term counts
	// 1 for the back index entry
	expectedLength := uint64(1 + 3 + 1 + (64 / document.DEFAULT_PRECISION_STEP) + (64 / document.DEFAULT_PRECISION_STEP) + 3 + 1 + (64 / document.DEFAULT_PRECISION_STEP) + (64 / document.DEFAULT_PRECISION_STEP) + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	storedDoc, err := idx.Document("1")
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 3 {
		t.Errorf("expected 3 stored field, got %d", len(storedDoc.Fields))
	}
	textField, ok := storedDoc.Fields[0].(*document.TextField)
	if !ok {
		t.Errorf("expected text field")
	}
	if string(textField.Value()) != "test" {
		t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
	}
	numField, ok := storedDoc.Fields[1].(*document.NumericField)
	if !ok {
		t.Errorf("expected numeric field")
	}
	numFieldNumer, err := numField.Number()
	if err != nil {
		t.Error(err)
	} else {
		if numFieldNumer != 35.99 {
			t.Errorf("expeted numeric value 35.99, got %f", numFieldNumer)
		}
	}
	dateField, ok := storedDoc.Fields[2].(*document.DateTimeField)
	if !ok {
		t.Errorf("expected date field")
	}
	dateFieldDate, err := dateField.DateTime()
	if err != nil {
		t.Error(err)
	} else {
		if dateFieldDate != time.Unix(0, 0) {
			t.Errorf("expected date value unix epoch, got %v", dateFieldDate)
		}
	}

	// now update the document, but omit one of the fields
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("testup"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 36.99, document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// expected doc count shouldn't have changed
	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should only get 2 fields back now though
	storedDoc, err = idx.Document("1")
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 2 {
		t.Errorf("expected 3 stored field, got %d", len(storedDoc.Fields))
	}
	textField, ok = storedDoc.Fields[0].(*document.TextField)
	if !ok {
		t.Errorf("expected text field")
	}
	if string(textField.Value()) != "testup" {
		t.Errorf("expected field content 'testup', got '%s'", string(textField.Value()))
	}
	numField, ok = storedDoc.Fields[1].(*document.NumericField)
	if !ok {
		t.Errorf("expected numeric field")
	}
	numFieldNumer, err = numField.Number()
	if err != nil {
		t.Error(err)
	} else {
		if numFieldNumer != 36.99 {
			t.Errorf("expeted numeric value 36.99, got %f", numFieldNumer)
		}
	}

	// now delete the document
	err = idx.Delete("1")
	expectedCount--

	// expected doc count shouldn't have changed
	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
}

func TestIndexInsertFields(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, document.INDEX_FIELD|document.STORE_FIELD))
	dateField, err := document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), document.INDEX_FIELD|document.STORE_FIELD)
	if err != nil {
		t.Error(err)
	}
	doc.AddField(dateField)
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	fields, err := idx.Fields()
	if err != nil {
		t.Error(err)
	} else {
		expectedFields := []string{"name", "age", "unixEpoch"}
		if !reflect.DeepEqual(fields, expectedFields) {
			t.Errorf("expected fields: %v, got %v", expectedFields, fields)
		}
	}

}

func TestIndexUpdateComposites(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, document.INDEX_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// should have 72 rows
	// 1 for version
	// 3 for schema fields
	// 4 for text term
	// 2 for the stored field
	// 4 for the text term count
	// 1 for the back index entry
	expectedLength := uint64(1 + 3 + 4 + 2 + 4 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// now lets update it
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("testupdated"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("misterupdated"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, document.INDEX_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// make sure new values are in index
	storedDoc, err := idx.Document("1")
	if err != nil {
		t.Error(err)
	}
	if len(storedDoc.Fields) != 2 {
		t.Errorf("expected 2 stored field, got %d", len(storedDoc.Fields))
	}
	textField, ok := storedDoc.Fields[0].(*document.TextField)
	if !ok {
		t.Errorf("expected text field")
	}
	if string(textField.Value()) != "testupdated" {
		t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
	}

	// should have the same row count as before
	rowCount = idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexFieldsMisc(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	fieldName1 := idx.fieldIndexToName(1)
	if fieldName1 != "name" {
		t.Errorf("expected field named 'name', got '%s'", fieldName1)
	}
	fieldName2 := idx.fieldIndexToName(2)
	if fieldName2 != "title" {
		t.Errorf("expected field named 'title', got '%s'", fieldName2)
	}
	fieldName3 := idx.fieldIndexToName(3)
	if fieldName3 != "" {
		t.Errorf("expected field named '', got '%s'", fieldName3)
	}

}

func TestIndexTermReaderCompositeFields(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD|document.INCLUDE_TERM_VECTORS))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), document.INDEX_FIELD|document.STORE_FIELD|document.INCLUDE_TERM_VECTORS))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, document.INDEX_FIELD|document.INCLUDE_TERM_VECTORS))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	termFieldReader, err := idx.TermFieldReader([]byte("mister"), "_all")
	if err != nil {
		t.Error(err)
	}

	tfd, err := termFieldReader.Next()
	for tfd != nil && err == nil {
		if tfd.ID != "1" {
			t.Errorf("expected to find document id 1")
		}
		tfd, err = termFieldReader.Next()
	}
	if err != nil {
		t.Error(err)
	}
}

func TestIndexDocumentFieldTerms(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := boltdb.Open("test", "bleve")
	if err != nil {
		t.Error(err)
	}
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), document.INDEX_FIELD|document.STORE_FIELD|document.INCLUDE_TERM_VECTORS))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), document.INDEX_FIELD|document.STORE_FIELD|document.INCLUDE_TERM_VECTORS))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	fieldTerms, err := idx.DocumentFieldTerms("1")
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := index.FieldTerms{
		"name":  []string{"test"},
		"title": []string{"mister"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
}
