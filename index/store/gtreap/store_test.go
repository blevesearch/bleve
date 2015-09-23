//  Copyright (c) 2014 Couchbase, Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package gtreap

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
)

func open(mo store.MergeOperator) (store.KVStore, error) {
	return New(mo, nil)
}

func TestGTreapKVCrud(t *testing.T) {
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

func TestGTreapReaderIsolation(t *testing.T) {
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

func TestGTreapReaderOwnsGetBytes(t *testing.T) {
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

func TestGTreapWriterOwnsBytes(t *testing.T) {
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

func TestGTreapPrefixIterator(t *testing.T) {
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

func TestGTreapRangeIterator(t *testing.T) {
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

func TestGTreapMerge(t *testing.T) {
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
