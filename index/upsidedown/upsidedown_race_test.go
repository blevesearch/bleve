//  Copyright (c) 2018 Couchbase, Inc.
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

// +build race

package upsidedown

import (
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/boltdb"
)

func TestDataRace(t *testing.T) {
	defer func() {
		err := DestroyTest()
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewUpsideDownCouch(boltdb.Name, boltTestConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for i := 0; i < 1000; i++ {
		err = idx.Batch(batch)
		if err != nil {
			t.Error(err)
		}

		batch.Reset()
	}
}
