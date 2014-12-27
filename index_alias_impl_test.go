package bleve

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/search"
)

func TestIndexAliasSingle(t *testing.T) {
	expectedError := fmt.Errorf("expected")
	ei1 := &stubIndex{
		err: expectedError,
	}

	alias := NewIndexAlias(ei1)

	err := alias.Index("a", "a")
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	err = alias.Delete("a")
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	err = alias.Batch(NewBatch())
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	_, err = alias.Document("a")
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	_, err = alias.Fields()
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	res := alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping := alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat := alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err = alias.Search(sr)
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	_, err = alias.DocCount()
	if err != expectedError {
		t.Errorf("expected %v, got %v", expectedError, err)
	}

	// now change the def using add/remove
	expectedError2 := fmt.Errorf("expected2")
	ei2 := &stubIndex{
		err: expectedError2,
	}

	alias.Add(ei2)
	alias.Remove(ei1)

	err = alias.Index("a", "a")
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	err = alias.Delete("a")
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	err = alias.Batch(NewBatch())
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	_, err = alias.Document("a")
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	_, err = alias.Fields()
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	res = alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping = alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat = alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	_, err = alias.Search(sr)
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	_, err = alias.DocCount()
	if err != expectedError2 {
		t.Errorf("expected %v, got %v", expectedError2, err)
	}

	// now change the def using swap
	expectedError3 := fmt.Errorf("expected3")
	ei3 := &stubIndex{
		err: expectedError3,
	}

	alias.Swap([]Index{ei3}, []Index{ei2})

	err = alias.Index("a", "a")
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	err = alias.Delete("a")
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	err = alias.Batch(NewBatch())
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	_, err = alias.Document("a")
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	_, err = alias.Fields()
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	res = alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping = alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat = alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	_, err = alias.Search(sr)
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}

	_, err = alias.DocCount()
	if err != expectedError3 {
		t.Errorf("expected %v, got %v", expectedError3, err)
	}
}

func TestIndexAliasClosed(t *testing.T) {
	alias := NewIndexAlias()
	alias.Close()

	err := alias.Index("a", "a")
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	err = alias.Delete("a")
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	err = alias.Batch(NewBatch())
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	_, err = alias.Document("a")
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	_, err = alias.Fields()
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	res := alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping := alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat := alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err = alias.Search(sr)
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}

	_, err = alias.DocCount()
	if err != ErrorIndexClosed {
		t.Errorf("expected %v, got %v", ErrorIndexClosed, err)
	}
}

func TestIndexAliasEmpty(t *testing.T) {
	alias := NewIndexAlias()

	err := alias.Index("a", "a")
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	err = alias.Delete("a")
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	err = alias.Batch(NewBatch())
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	_, err = alias.Document("a")
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	_, err = alias.Fields()
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	res := alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping := alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat := alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err = alias.Search(sr)
	if err != ErrorAliasEmpty {
		t.Errorf("expected %v, got %v", ErrorAliasEmpty, err)
	}

	count, err := alias.DocCount()
	if count != 0 {
		t.Errorf("expected %d, got %d", 0, count)
	}
}

func TestIndexAliasMulti(t *testing.T) {
	ei1Count := uint64(7)
	ei1 := &stubIndex{
		err:            nil,
		docCountResult: &ei1Count,
		searchResult: &SearchResult{
			Total: 1,
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{
					ID:    "a",
					Score: 1.0,
				},
			},
			Took:     1 * time.Second,
			MaxScore: 1.0,
		}}
	ei2Count := uint64(8)
	ei2 := &stubIndex{
		err:            nil,
		docCountResult: &ei2Count,
		searchResult: &SearchResult{
			Total: 1,
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{
					ID:    "b",
					Score: 2.0,
				},
			},
			Took:     2 * time.Second,
			MaxScore: 2.0,
		}}

	alias := NewIndexAlias(ei1, ei2)

	err := alias.Index("a", "a")
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	err = alias.Delete("a")
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	err = alias.Batch(NewBatch())
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	_, err = alias.Document("a")
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	_, err = alias.Fields()
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	_, err = alias.GetInternal([]byte("a"))
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	err = alias.SetInternal([]byte("a"), []byte("a"))
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	err = alias.DeleteInternal([]byte("a"))
	if err != ErrorAliasMulti {
		t.Errorf("expected %v, got %v", ErrorAliasMulti, err)
	}

	res := alias.DumpAll()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpDoc("a")
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	res = alias.DumpFields()
	if res != nil {
		t.Errorf("expected nil, got %v", res)
	}

	mapping := alias.Mapping()
	if mapping != nil {
		t.Errorf("expected nil, got %v", res)
	}

	indexStat := alias.Stats()
	if indexStat != nil {
		t.Errorf("expected nil, got %v", res)
	}

	// now a few things that should work
	sr := NewSearchRequest(NewTermQuery("test"))
	expected := &SearchResult{
		Total: 2,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "b",
				Score: 2.0,
			},
			&search.DocumentMatch{
				ID:    "a",
				Score: 1.0,
			},
		},
		Took:     3 * time.Second,
		MaxScore: 2.0,
	}
	results, err := alias.Search(sr)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %#v, got %#v", expected, results)
	}

	count, err := alias.DocCount()
	if count != (*ei1.docCountResult + *ei2.docCountResult) {
		t.Errorf("expected %d, got %d", (*ei1.docCountResult + *ei2.docCountResult), count)
	}
}

// TestMultiSearchNoError
func TestMultiSearchNoError(t *testing.T) {
	ei1 := &stubIndex{err: nil, searchResult: &SearchResult{
		Total: 1,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 1.0,
			},
		},
		Took:     1 * time.Second,
		MaxScore: 1.0,
	}}
	ei2 := &stubIndex{err: nil, searchResult: &SearchResult{
		Total: 1,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "b",
				Score: 2.0,
			},
		},
		Took:     2 * time.Second,
		MaxScore: 2.0,
	}}

	expected := &SearchResult{
		Total: 2,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "b",
				Score: 2.0,
			},
			&search.DocumentMatch{
				ID:    "a",
				Score: 1.0,
			},
		},
		Took:     3 * time.Second,
		MaxScore: 2.0,
	}

	sr := NewSearchRequest(NewTermQuery("test"))
	results, err := MultiSearch(sr, ei1, ei2)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %#v, got %#v", expected, results)
	}
}

// TestMultiSearchSomeError
func TestMultiSearchSomeError(t *testing.T) {
	ei1 := &stubIndex{err: nil, searchResult: &SearchResult{
		Total: 1,
		Hits: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 1.0,
			},
		},
		Took:     1 * time.Second,
		MaxScore: 1.0,
	}}
	ei2 := &stubIndex{err: fmt.Errorf("deliberate error")}
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err := MultiSearch(sr, ei1, ei2)
	if err == nil {
		t.Errorf("expected error, got %v", err)
	}
}

// TestMultiSearchAllError
// reproduces https://github.com/blevesearch/bleve/issues/126
func TestMultiSearchAllError(t *testing.T) {
	ei1 := &stubIndex{err: fmt.Errorf("deliberate error")}
	ei2 := &stubIndex{err: fmt.Errorf("deliberate error")}
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err := MultiSearch(sr, ei1, ei2)
	if err == nil {
		t.Errorf("expected error, got %v", err)
	}
}

// stubIndex is an Index impl for which all operations
// return the configured error value, unless the
// corresponding operation result value has been
// set, in which case that is returned instead
type stubIndex struct {
	err            error
	searchResult   *SearchResult
	documentResult *document.Document
	docCountResult *uint64
}

func (i *stubIndex) Index(id string, data interface{}) error {
	return i.err
}

func (i *stubIndex) Delete(id string) error {
	return i.err
}

func (i *stubIndex) Batch(b *Batch) error {
	return i.err
}

func (i *stubIndex) Document(id string) (*document.Document, error) {
	if i.documentResult != nil {
		return i.documentResult, nil
	}
	return nil, i.err
}

func (i *stubIndex) DocCount() (uint64, error) {
	if i.docCountResult != nil {
		return *i.docCountResult, nil
	}
	return 0, i.err
}

func (i *stubIndex) Search(req *SearchRequest) (*SearchResult, error) {
	if i.searchResult != nil {
		return i.searchResult, nil
	}
	return nil, i.err
}

func (i *stubIndex) Fields() ([]string, error) {
	return nil, i.err
}

func (i *stubIndex) DumpAll() chan interface{} {
	return nil
}

func (i *stubIndex) DumpDoc(id string) chan interface{} {
	return nil
}

func (i *stubIndex) DumpFields() chan interface{} {
	return nil
}

func (i *stubIndex) Close() error {
	return i.err
}

func (i *stubIndex) Mapping() *IndexMapping {
	return nil
}

func (i *stubIndex) Stats() *IndexStat {
	return nil
}

func (i *stubIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, i.err
}

func (i *stubIndex) SetInternal(key, val []byte) error {
	return i.err
}

func (i *stubIndex) DeleteInternal(key []byte) error {
	return i.err
}

func (i *stubIndex) Advanced() (index.Index, store.KVStore, error) {
	return nil, nil, nil
}
