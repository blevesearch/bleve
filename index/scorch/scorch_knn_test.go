//  Copyright (c) 2026 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package scorch

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

const (
	testVectorDims              = 3
	testVectorSimilarity        = index.CosineSimilarity
	testVectorIndexOptimizedFor = index.IndexOptimizedForRecall
)

func genVectorDoc(t *testing.T, fieldName string) index.Document {
	t.Helper()
	randNum := rand.Intn(1000000)
	doc := document.NewDocument(fmt.Sprintf("vectest%d", randNum))
	vector := make([]float32, testVectorDims)
	for i := range vector {
		vector[i] = float32(i+1) * float32(randNum+1)
	}
	doc.AddField(document.NewVectorField(fieldName, nil, vector, testVectorDims, testVectorSimilarity, testVectorIndexOptimizedFor))
	doc.AddIDField()
	doc.VisitFields(func(field index.Field) {
		field.Analyze()
	})
	return doc
}

func TestFieldStatPersistence(t *testing.T) {
	cfg := CreateConfig("TestFieldStatPersistence")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", idx)
	}
	s.path = dirPath
	if err = s.openBolt(); err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	const (
		numSegments = 3
		fieldName   = "vector"
		statName    = "field:" + fieldName + ":num_vectors"
	)
	docs := make([]index.Document, numSegments)
	ids := make([]string, numSegments)
	for i := 0; i < numSegments; i++ {
		docs[i] = genVectorDoc(t, fieldName)
		ids[i] = docs[i].ID()
		seg, _, err := s.segPlugin.New([]index.Document{docs[i]})
		if err != nil {
			t.Fatalf("failed to create segment %d: %v", i, err)
		}
		intro := &segmentIntroduction{
			id:      atomic.AddUint64(&s.nextSegmentID, 1),
			data:    seg,
			ids:     []string{ids[i]},
			applied: make(chan error),
		}
		if err = s.introduceSegment(intro); err != nil {
			t.Fatalf("introduceSegment %d failed: %v", i, err)
		}
	}

	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	go func() {
		snapshot := s.root
		snapshot.AddRef()
		defer snapshot.DecRef()
		err := s.persistSnapshot(snapshot, s.persisterOptions)
		if err != nil {
			errCh <- err
			return
		}
		doneCh <- struct{}{}
	}()
	select {
	case merge := <-s.merges:
		s.introduceMerge(merge)
	case err := <-errCh:
		t.Fatalf("unexpected error during persist (merge phase): %v", err)
	}
	<-doneCh
	snapshot := s.root
	if len(snapshot.segment) != 1 {
		t.Fatalf("expected 1 segment after introduce, got %d", len(snapshot.segment))
	}
	if !snapshot.segment[0].HasVector() {
		t.Fatalf("expected HasVector() == true after index persist")
	}
	scorchStats := s.StatsMap()
	numVecs := scorchStats[statName].(uint64)
	if numVecs != uint64(numSegments) {
		t.Fatalf("expected %d vectors in stats, got %d", numSegments, numVecs)
	}
	if err = idx.Close(); err != nil {
		t.Fatalf("failed to close index: %v", err)
	}
	idx, err = NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to recreate Scorch: %v", err)
	}
	s, ok = idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", idx)
	}
	s.path = dirPath
	if err = s.openBolt(); err != nil {
		t.Fatalf("failed to open bolt on reopen: %v", err)
	}
	snapshot = s.root
	if len(snapshot.segment) != 1 {
		t.Fatalf("expected 1 segment after reopen, got %d", len(snapshot.segment))
	}
	if !snapshot.segment[0].HasVector() {
		t.Fatalf("reopened segment: expected HasVector() == true after index reopen")
	}
	scorchStats = s.StatsMap()
	numVecs = scorchStats[statName].(uint64)
	if numVecs != uint64(numSegments) {
		t.Fatalf("expected %d vectors in stats, got %d", numSegments, numVecs)
	}
	err = idx.Close()
	if err != nil {
		t.Fatalf("failed to close index on reopen: %v", err)
	}
}
