package null

import (
	"testing"

	"github.com/blevesearch/bleve/index/store"
)

func TestStore(t *testing.T) {
	s, err := New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	NullTestKVStore(t, s)
}

// NullTestKVStore has very different expectations
// compared to CommonTestKVStore
func NullTestKVStore(t *testing.T, s store.KVStore) {

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}

	batch := writer.NewBatch()
	batch.Set([]byte("b"), []byte("val-b"))
	batch.Set([]byte("c"), []byte("val-c"))
	batch.Set([]byte("d"), []byte("val-d"))
	batch.Set([]byte("e"), []byte("val-e"))
	batch.Set([]byte("f"), []byte("val-f"))
	batch.Set([]byte("g"), []byte("val-g"))
	batch.Set([]byte("h"), []byte("val-h"))
	batch.Set([]byte("i"), []byte("val-i"))
	batch.Set([]byte("j"), []byte("val-j"))

	err = writer.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	it := reader.RangeIterator([]byte("b"), nil)
	key, val, valid := it.Current()
	if valid {
		t.Fatalf("valid true, expected false")
	}
	if key != nil {
		t.Fatalf("expected key nil, got %s", key)
	}
	if val != nil {
		t.Fatalf("expected value nil, got %s", val)
	}

	err = it.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
}
