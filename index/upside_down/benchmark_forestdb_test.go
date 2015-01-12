//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

package upside_down

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/forestdb"
)

func BenchmarkForestDBIndexing1Workers(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s, 1)
}

func BenchmarkForestDBIndexing2Workers(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s, 2)
}

func BenchmarkForestDBIndexing4Workers(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndex(b, s, 4)
}

// batches

func BenchmarkForestDBIndexing1Workers10Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 1, 10)
}

func BenchmarkForestDBIndexing2Workers10Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 2, 10)
}

func BenchmarkForestDBIndexing4Workers10Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 4, 10)
}

func BenchmarkForestDBIndexing1Workers100Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 1, 100)
}

func BenchmarkForestDBIndexing2Workers100Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 2, 100)
}

func BenchmarkForestDBIndexing4Workers100Batch(b *testing.B) {
	s, err := forestdb.Open("test", true)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll("test")
	defer s.Close()

	CommonBenchmarkIndexBatch(b, s, 4, 100)
}
