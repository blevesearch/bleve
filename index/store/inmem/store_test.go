package inmem

import (
	"testing"

	"github.com/couchbaselabs/bleve/index/store/test"
)

func TestInMemStore(t *testing.T) {
	s, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store_test.CommonTestKVStore(t, s)
}
