package null

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
)

func TestStore(t *testing.T) {
	s, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("test")

	CommonTestKVStore(t, s)
}

func CommonTestKVStore(t *testing.T, s store.KVStore) {

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Set([]byte("z"), []byte("val-z"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Delete([]byte("z"))
	if err != nil {
		t.Fatal(err)
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

	err = batch.Execute()
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()
	it := reader.Iterator([]byte("b"))
	key, val, valid := it.Current()
	if valid {
		t.Fatalf("valid true, expected false")
	}
	if key != nil {
		t.Fatalf("exepcted key nil, got %s", key)
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
