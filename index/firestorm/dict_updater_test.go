//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"runtime"
	"testing"
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
)

func TestDictUpdater(t *testing.T) {
	aq := index.NewAnalysisQueue(1)
	f, err := NewFirestorm(gtreap.Name, nil, aq)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Open()
	if err != nil {
		t.Fatal(err)
	}

	dictBatch := map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): 3,
	}
	dictExpect := map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): 3,
	}

	f.(*Firestorm).dictUpdater.NotifyBatch(dictBatch)

	// invoke updater manually
	for len(f.(*Firestorm).dictUpdater.incoming) > 0 {
		runtime.Gosched()
	}
	err = f.(*Firestorm).dictUpdater.waitTasksDone(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// assert that dictionary rows are correct
	reader, err := f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	for key := range dictBatch {
		v, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if v == nil {
			t.Fatal("unexpected dictionary value missing")
		}
		dr, err := NewDictionaryRowKV([]byte(key), v)
		if err != nil {
			t.Fatal(err)
		}
		expect := dictExpect[key]
		if int64(dr.Count()) != expect {
			t.Errorf("expected %d, got %d", expect, dr.Count())
		}
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// update it again
	dictBatch = map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): 1,
	}
	dictExpect = map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): 4,
	}

	f.(*Firestorm).dictUpdater.NotifyBatch(dictBatch)

	// invoke updater manually
	for len(f.(*Firestorm).dictUpdater.incoming) > 0 {
		runtime.Gosched()
	}
	err = f.(*Firestorm).dictUpdater.waitTasksDone(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// assert that dictionary rows are correct
	reader, err = f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	for key := range dictBatch {
		v, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		dr, err := NewDictionaryRowKV([]byte(key), v)
		if err != nil {
			t.Fatal(err)
		}
		expect := dictExpect[key]
		if int64(dr.Count()) != expect {
			t.Errorf("expected %d, got %d", expect, dr.Count())
		}
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// update it again (decrement this time)
	dictBatch = map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): -2,
	}
	dictExpect = map[string]int64{
		string([]byte{'d', 1, 0, 'c', 'a', 't'}): 2,
	}

	f.(*Firestorm).dictUpdater.NotifyBatch(dictBatch)

	// invoke updater manually
	for len(f.(*Firestorm).dictUpdater.incoming) > 0 {
		runtime.Gosched()
	}
	err = f.(*Firestorm).dictUpdater.waitTasksDone(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// assert that dictionary rows are correct
	reader, err = f.(*Firestorm).store.Reader()
	if err != nil {
		t.Fatal(err)
	}

	for key := range dictBatch {
		v, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		dr, err := NewDictionaryRowKV([]byte(key), v)
		if err != nil {
			t.Fatal(err)
		}
		expect := dictExpect[key]
		if int64(dr.Count()) != expect {
			t.Errorf("expected %d, got %d", expect, dr.Count())
		}
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}
