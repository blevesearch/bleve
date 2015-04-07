//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build leveldb full

package upside_down

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/leveldb"
)

var leveldbTestOptions = map[string]interface{}{
	"create_if_missing": true,
}

func BenchmarkLevelDBIndexing1Workers(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndex(b, s, 1)
}

func BenchmarkLevelDBIndexing2Workers(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndex(b, s, 2)
}

func BenchmarkLevelDBIndexing4Workers(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndex(b, s, 4)
}

// batches

func BenchmarkLevelDBIndexing1Workers10Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 1, 10)
}

func BenchmarkLevelDBIndexing2Workers10Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 2, 10)
}

func BenchmarkLevelDBIndexing4Workers10Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 4, 10)
}

func BenchmarkLevelDBIndexing1Workers100Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 1, 100)
}

func BenchmarkLevelDBIndexing2Workers100Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 2, 100)
}

func BenchmarkLevelDBIndexing4Workers100Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 4, 100)
}

func BenchmarkLevelDBIndexing1Workers1000Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 1, 1000)
}

func BenchmarkLevelDBIndexing2Workers1000Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 2, 1000)
}

func BenchmarkLevelDBIndexing4Workers1000Batch(b *testing.B) {
	s, err := leveldb.Open("test", leveldbTestOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer func() {
		err := s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	CommonBenchmarkIndexBatch(b, s, 4, 1000)
}
