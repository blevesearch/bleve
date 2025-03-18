//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorch

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

func TestBuilder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "scorch-builder-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("error cleaning up test index: %v", err)
		}
	}()
	options := map[string]interface{}{
		"path":      tmpDir,
		"batchSize": 2,
		"mergeMax":  2,
	}
	b, err := NewBuilder(options)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		doc := document.NewDocument(fmt.Sprintf("%d", i))
		doc.AddField(document.NewTextField("name", nil, []byte("hello")))
		err = b.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}

	checkIndex(t, tmpDir, []byte("hello"), "name", 10)
}

func checkIndex(t *testing.T, path string, term []byte, field string, expectCount int) {
	cfg := make(map[string]interface{})
	cfg["path"] = path
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatalf("error closing index: %v", err)
		}
	}()

	r, err := idx.Reader()
	if err != nil {
		t.Fatalf("error accessing index reader: %v", err)
	}
	defer func() {
		err = r.Close()
		if err != nil {
			t.Fatalf("error closing reader: %v", err)
		}
	}()

	// check the count, expect 10 docs
	count, err := r.DocCount()
	if err != nil {
		t.Errorf("error accessing index doc count: %v", err)
	} else if count != uint64(expectCount) {
		t.Errorf("expected %d docs, got %d", expectCount, count)
	}

	// run a search for hello
	tfr, err := r.TermFieldReader(context.TODO(), term, field, false, false, false)
	if err != nil {
		t.Errorf("error accessing term field reader: %v", err)
	} else {
		var rows int
		tfd, err := tfr.Next(nil)
		for err == nil && tfd != nil {
			rows++
			tfd, err = tfr.Next(nil)
		}
		if err != nil {
			t.Errorf("error calling next on term field reader: %v", err)
		}
		if rows != expectCount {
			t.Errorf("expected %d rows for term hello, field name, got %d", expectCount, rows)
		}
	}
}

func TestBuilderFlushFinalBatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "scorch-builder-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("error cleaning up test index: %v", err)
		}
	}()
	options := map[string]interface{}{
		"path":      tmpDir,
		"batchSize": 2,
		"mergeMax":  2,
	}
	b, err := NewBuilder(options)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 9; i++ {
		doc := document.NewDocument(fmt.Sprintf("%d", i))
		doc.AddField(document.NewTextField("name", nil, []byte("hello")))
		err = b.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}

	checkIndex(t, tmpDir, []byte("hello"), "name", 9)
}
