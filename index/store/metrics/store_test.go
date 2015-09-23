package metrics

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/store/test"
)

func open(mo store.MergeOperator) (store.KVStore, error) {
	return New(mo, map[string]interface{}{"kvStoreName_actual": gtreap.Name})
}

func TestMetricsKVCrud(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestKVCrud(t, s)
}

func TestMetricsReaderIsolation(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestReaderIsolation(t, s)
}

func TestMetricsReaderOwnsGetBytes(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestMetricsWriterOwnsBytes(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestWriterOwnsBytes(t, s)
}

func TestMetricsPrefixIterator(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestPrefixIterator(t, s)
}

func TestMetricsRangeIterator(t *testing.T) {
	s, err := open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestRangeIterator(t, s)
}

func TestMetricsMerge(t *testing.T) {
	s, err := open(&test.TestMergeCounter{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	test.CommonTestMerge(t, s)
}
