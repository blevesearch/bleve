package bleve

import (
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/document"
)

// TestMultiSearchAllError
// reproduces https://github.com/blevesearch/bleve/issues/126
func TestMultiSearchAllError(t *testing.T) {
	ei1 := &errIndex{err: fmt.Errorf("deliberate error")}
	ei2 := &errIndex{err: fmt.Errorf("deliberate error")}
	sr := NewSearchRequest(NewTermQuery("test"))
	_, err := MultiSearch(sr, ei1, ei2)
	if err == nil {
		t.Errorf("expected error, got %v", err)
	}
}

// errIndex is an Index impl for which all operations
// return the configured error value
type errIndex struct {
	err error
}

func (i *errIndex) Index(id string, data interface{}) error {
	return i.err
}

func (i *errIndex) Delete(id string) error {
	return i.err
}

func (i *errIndex) Batch(b *Batch) error {
	return i.err
}

func (i *errIndex) Document(id string) (*document.Document, error) {
	return nil, i.err
}

func (i *errIndex) DocCount() (uint64, error) {
	return 0, i.err
}

func (i *errIndex) Search(req *SearchRequest) (*SearchResult, error) {
	return nil, i.err
}

func (i *errIndex) Fields() ([]string, error) {
	return nil, i.err
}

func (i *errIndex) DumpAll() chan interface{} {
	return nil
}

func (i *errIndex) DumpDoc(id string) chan interface{} {
	return nil
}

func (i *errIndex) DumpFields() chan interface{} {
	return nil
}

func (i *errIndex) Close() error {
	return i.err
}

func (i *errIndex) Mapping() *IndexMapping {
	return nil
}

func (i *errIndex) Stats() *IndexStat {
	return nil
}

func (i *errIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, i.err
}

func (i *errIndex) SetInternal(key, val []byte) error {
	return i.err
}

func (i *errIndex) DeleteInternal(key []byte) error {
	return i.err
}
