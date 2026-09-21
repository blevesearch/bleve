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

package scorch

import (
	"context"
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

// TestAdvanceDocNum checks AdvanceDocNum against the generic, byte-encoded
// Advance for the exact same forward-only target sequence, across a term
// that spans several segments (one doc per Update call, like
// TestIndexSeekBackwardsStats relies on to get separate segments) -- the
// case AdvanceDocNum's shared advanceNum core has to get right that a
// single-segment index never exercises: falling through to Next() when the
// segment a target's global doc number maps to does not itself contain a
// hit, so the next real hit lives in a later segment entirely.
func TestAdvanceDocNum(t *testing.T) {
	cfg := CreateConfig("TestAdvanceDocNum")
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := DestroyTest(cfg); err != nil {
			t.Log(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	const numDocs = 8
	for i := 0; i < numDocs; i++ {
		doc := document.NewDocument(fmt.Sprintf("d%d", i))
		doc.AddField(document.NewTextField("name", []uint64{}, []byte("cat")))
		if err := idx.Update(doc); err != nil {
			t.Fatalf("error updating index: %v", err)
		}
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}
	defer reader.Close()

	// Discover the real model: global doc numbers, freqs and norms for
	// "cat", via a plain forward scan.
	type posting struct {
		docNum uint64
		freq   uint64
		norm   float64
	}
	scan, err := reader.TermFieldReader(context.TODO(), []byte("cat"), "name", true, true, false)
	if err != nil {
		t.Fatalf("error getting term field reader: %v", err)
	}
	var want []posting
	for {
		tfd, err := scan.Next(nil)
		if err != nil {
			t.Fatalf("error scanning: %v", err)
		}
		if tfd == nil {
			break
		}
		want = append(want, posting{tfd.ID.Value(), tfd.Freq, tfd.Norm})
	}
	if err := scan.Close(); err != nil {
		t.Fatalf("error closing scan reader: %v", err)
	}
	if len(want) != numDocs {
		t.Fatalf("got %d postings for \"cat\", want %d", len(want), numDocs)
	}

	// One probe per want[] entry: for every other entry whose docNum isn't
	// adjacent to the previous one, probe the gap right before it instead of
	// the entry's own docNum, exercising "first document at or after"
	// alongside exact hits -- same construction TestFillTermFieldDocSeek
	// uses in zapx, and for the same reason (never probe the same entry
	// twice, which would be a backward-or-equal seek).
	var targets []uint64
	prevDocNum := uint64(0)
	for i, w := range want {
		if i%2 == 1 && w.docNum > 0 && (i == 0 || w.docNum-1 > prevDocNum) {
			targets = append(targets, w.docNum-1)
		} else {
			targets = append(targets, w.docNum)
		}
		prevDocNum = w.docNum
	}

	generic, err := reader.TermFieldReader(context.TODO(), []byte("cat"), "name", true, true, false)
	if err != nil {
		t.Fatalf("error getting generic reader: %v", err)
	}
	defer generic.Close()
	fast, err := reader.TermFieldReader(context.TODO(), []byte("cat"), "name", true, true, false)
	if err != nil {
		t.Fatalf("error getting fast reader: %v", err)
	}
	defer fast.Close()
	fastTFR, ok := fast.(*IndexSnapshotTermFieldReader)
	if !ok {
		t.Fatalf("reader is %T, not *IndexSnapshotTermFieldReader", fast)
	}

	var idBuf index.IndexInternalID
	for i, target := range targets {
		w := want[i]

		idBuf = index.NewIndexInternalID(idBuf, target)
		gtfd, err := generic.Advance(idBuf, nil)
		if err != nil {
			t.Fatalf("target %d: Advance error: %v", target, err)
		}
		if gtfd == nil || gtfd.ID.Value() != w.docNum || gtfd.Freq != w.freq || gtfd.Norm != w.norm {
			t.Fatalf("target %d: Advance = %+v, want docNum=%d freq=%d norm=%v",
				target, gtfd, w.docNum, w.freq, w.norm)
		}

		docNum, freq, norm, exists, err := fastTFR.AdvanceDocNum(target)
		if err != nil {
			t.Fatalf("target %d: AdvanceDocNum error: %v", target, err)
		}
		if !exists {
			t.Fatalf("target %d: AdvanceDocNum reported not found, want docNum %d", target, w.docNum)
		}
		if docNum != w.docNum || freq != w.freq || norm != w.norm {
			t.Fatalf("target %d: AdvanceDocNum = (docNum=%d freq=%d norm=%v), want (docNum=%d freq=%d norm=%v)",
				target, docNum, freq, norm, w.docNum, w.freq, w.norm)
		}
	}

	// Past the last real posting, both must report not-found.
	lastTarget := want[len(want)-1].docNum + 1
	idBuf = index.NewIndexInternalID(idBuf, lastTarget)
	if gtfd, err := generic.Advance(idBuf, nil); err != nil || gtfd != nil {
		t.Fatalf("post-exhaustion Advance: got %+v, err %v, want nil/nil", gtfd, err)
	}
	if _, _, _, exists, err := fastTFR.AdvanceDocNum(lastTarget); err != nil || exists {
		t.Fatalf("post-exhaustion AdvanceDocNum: exists=%v, err %v, want false/nil", exists, err)
	}
}
