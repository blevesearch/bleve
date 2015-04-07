//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/boltdb"
)

func BenchmarkBoltDBIndexing1Workers(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndex(b, s, 1)
}

func BenchmarkBoltDBIndexing2Workers(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndex(b, s, 2)
}

func BenchmarkBoltDBIndexing4Workers(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndex(b, s, 4)
}

// batches

func BenchmarkBoltDBIndexing1Workers10Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 1, 10)
}

func BenchmarkBoltDBIndexing2Workers10Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 2, 10)
}

func BenchmarkBoltDBIndexing4Workers10Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 4, 10)
}

func BenchmarkBoltDBIndexing1Workers100Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 1, 100)
}

func BenchmarkBoltDBIndexing2Workers100Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 2, 100)
}

func BenchmarkBoltDBIndexing4Workers100Batch(b *testing.B) {
	s, err := boltdb.Open("test", "bleve")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll("test")
		if err != nil {
			b.Fatal(err)
		}
	}()
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 4, 100)
}
