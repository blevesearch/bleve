//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

package smolder

import (
	"os"
	"testing"

	"github.com/blevesearch/blevex/forestdb"
)

var forestDBTestOption = map[string]interface{}{
	"path":              "testdir/test",
	"create_if_missing": true,
}

// internally used to reset, so we also
// re-make the testdir
func DestroyForestDB() error {
	err := os.RemoveAll("testdir")
	if err != nil {
		return err
	}
	err = os.MkdirAll("testdir", 0700)
	if err != nil {
		return err
	}
	return nil
}

func BenchmarkForestDBIndexing1Workers(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndex(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 1)
}

func BenchmarkForestDBIndexing2Workers(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndex(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 2)
}

func BenchmarkForestDBIndexing4Workers(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndex(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 4)
}

// batches

func BenchmarkForestDBIndexing1Workers10Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 1, 10)
}

func BenchmarkForestDBIndexing2Workers10Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 2, 10)
}

func BenchmarkForestDBIndexing4Workers10Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 4, 10)
}

func BenchmarkForestDBIndexing1Workers100Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 1, 100)
}

func BenchmarkForestDBIndexing2Workers100Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 2, 100)
}

func BenchmarkForestDBIndexing4Workers100Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 4, 100)
}

func BenchmarkForestDBIndexing1Workers1000Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 1, 1000)
}

func BenchmarkForestDBIndexing2Workers1000Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 2, 1000)
}

func BenchmarkForestDBIndexing4Workers1000Batch(b *testing.B) {
	err := os.MkdirAll("testdir", 0700)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}()
	CommonBenchmarkIndexBatch(b, forestdb.Name, forestDBTestOption, DestroyForestDB, 4, 1000)
}
