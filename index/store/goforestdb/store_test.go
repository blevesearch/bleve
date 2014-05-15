// +build forestdb

package goforestdb

import (
	"os"
	"testing"

	"github.com/couchbaselabs/bleve/index/store/test"
)

func TestForestDBStore(t *testing.T) {
	s, err := Open("test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("test")

	store_test.CommonTestKVStore(t, s)
}
