//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// this file includes tests which intentionally create race conditions

// +build !race

package bleve

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/index/upside_down"
)

func TestBatchCrashBug195(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	b := index.NewBatch()
	for i := 0; i < 200; i++ {
		b.Index(fmt.Sprintf("%d", i), struct {
			Value string
		}{
			Value: fmt.Sprintf("%d", i),
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := index.Batch(b)
		if err != nil && err != upside_down.UnsafeBatchUseDetected {
			t.Fatal(err)
		}
	}()

	// now keep adding to the batch after we've started to execute it
	for i := 200; i < 400; i++ {
		b.Index(fmt.Sprintf("%d", i), struct {
			Value string
		}{
			Value: fmt.Sprintf("%d", i),
		})
	}

	wg.Wait()

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}
