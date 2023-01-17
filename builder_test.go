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

package bleve

import (
	"fmt"
	"os"
	"testing"
)

func TestBuilder(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bleve-scorch-builder-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			t.Fatalf("error cleaning up test index")
		}
	}()

	conf := map[string]interface{}{
		"batchSize": 2,
		"mergeMax":  2,
	}
	b, err := NewBuilder(tmpDir, NewIndexMapping(), conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		doc := map[string]interface{}{
			"name": "hello",
		}
		err = b.Index(fmt.Sprintf("%d", i), doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Errorf("error closing index: %v", err)
		}
	}()

	docCount, err := idx.DocCount()
	if err != nil {
		t.Errorf("error checking doc count: %v", err)
	}
	if docCount != 10 {
		t.Errorf("expected doc count to be 10, got %d", docCount)
	}

	q := NewTermQuery("hello")
	q.SetField("name")
	req := NewSearchRequest(q)
	res, err := idx.Search(req)
	if err != nil {
		t.Errorf("error searching index: %v", err)
	}
	if res.Total != 10 {
		t.Errorf("expected 10 search hits, got %d", res.Total)
	}
}
