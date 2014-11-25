package bleve

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/search"
)

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
