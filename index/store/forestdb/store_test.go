//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

package forestdb

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/test"
)

func TestLevelDBStore(t *testing.T) {
	defer os.RemoveAll("test")

	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store_test.CommonTestKVStore(t, s)
}

func TestReaderIsolation(t *testing.T) {
	defer os.RemoveAll("test")

	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store_test.CommonTestReaderIsolation(t, s)
}

// TestRollbackSameHandle tries to rollback a handle
// and ensure that subsequent reads from it also
// reflect the rollback
func TestRollbackSameHandle(t *testing.T) {
	defer os.RemoveAll("test")

	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writer, err := s.Writer()
	if err != nil {
		t.Fatal(err)
	}

	// create 2 docs a and b
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Set([]byte("b"), []byte("val-b"))
	if err != nil {
		t.Error(err)
	}

	// get the rollback id
	rollbackId, err := s.getRollbackID()
	if err != nil {
		t.Error(err)
	}

	// create a 3rd doc c
	err = writer.Set([]byte("c"), []byte("val-c"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// make sure c is there
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err := reader.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "val-c" {
		t.Errorf("expected value 'val-c' got '%s'", val)
	}
	reader.Close()

	// now rollback
	err = s.rollbackTo(rollbackId)
	if err != nil {
		t.Fatal(err)
	}

	// now make sure c is not there
	reader, err = s.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err = reader.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected missing, got '%s'", val)
	}
	reader.Close()
}

// TestRollbackNewHandle tries to rollback the
// database, then open a new handle, and ensure
// that the rollback is reflected there as well
func TestRollbackNewHandle(t *testing.T) {
	defer os.RemoveAll("test")

	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writer, err := s.Writer()
	if err != nil {
		t.Fatal(err)
	}

	// create 2 docs a and b
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Set([]byte("b"), []byte("val-b"))
	if err != nil {
		t.Error(err)
	}

	// get the rollback id
	rollbackId, err := s.getRollbackID()
	if err != nil {
		t.Error(err)
	}

	// create a 3rd doc c
	err = writer.Set([]byte("c"), []byte("val-c"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// make sure c is there
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err := reader.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "val-c" {
		t.Errorf("expected value 'val-c' got '%s'", val)
	}
	reader.Close()

	// now rollback
	err = s.rollbackTo(rollbackId)
	if err != nil {
		t.Fatal(err)
	}

	// now lets open another handle
	s2, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// now make sure c is not there
	reader2, err := s2.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err = reader2.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected missing, got '%s'", val)
	}
	reader2.Close()
}

// TestRollbackOtherHandle tries to create 2 handles
// at the begining, then rollback one of them
// and ensure it affects the other
func TestRollbackOtherHandle(t *testing.T) {
	defer os.RemoveAll("test")

	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// open another handle at the same time
	s2, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	writer, err := s.Writer()
	if err != nil {
		t.Fatal(err)
	}

	// create 2 docs a and b
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Set([]byte("b"), []byte("val-b"))
	if err != nil {
		t.Error(err)
	}

	// get the rollback id
	rollbackId, err := s.getRollbackID()
	if err != nil {
		t.Error(err)
	}

	// create a 3rd doc c
	err = writer.Set([]byte("c"), []byte("val-c"))
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// make sure c is there
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err := reader.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "val-c" {
		t.Errorf("expected value 'val-c' got '%s'", val)
	}
	reader.Close()

	// now rollback
	err = s.rollbackTo(rollbackId)
	if err != nil {
		t.Fatal(err)
	}

	// now make sure c is not on the other handle
	reader2, err := s2.Reader()
	if err != nil {
		t.Error(err)
	}
	val, err = reader2.Get([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected missing, got '%s'", val)
	}
	reader2.Close()
}
