package scorch

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

var (
	benchDisjOnce sync.Once
	benchDisjIdx  index.Index
)

// 8 segments: segments 0-3 each contain one hot term (t0..t3, 500 docs each);
// segments 4-7 contain only "filler" — empty for every query term, exercising
// the empty-segment path in OptimizeTFRDisjunctionUnadorned.Finish().
func getBenchDisjIdx(b *testing.B) index.Index {
	benchDisjOnce.Do(func() {
		cfg := CreateConfig("BenchDisjUnadorned")
		if err := InitTest(cfg); err != nil {
			b.Fatal(err)
		}
		aq := index.NewAnalysisQueue(1)
		idx, err := NewScorch(Name, cfg, aq)
		if err != nil {
			b.Fatal(err)
		}
		if err := idx.Open(); err != nil {
			b.Fatal(err)
		}
		for seg := 0; seg < 8; seg++ {
			batch := index.NewBatch()
			for d := 0; d < 500; d++ {
				doc := document.NewDocument(fmt.Sprintf("%d-%d", seg, d))
				term := "filler"
				if seg < 4 {
					term = fmt.Sprintf("t%d", seg)
				}
				doc.AddField(document.NewTextField("f", []uint64{}, []byte(term)))
				batch.Update(doc)
			}
			if err := idx.Batch(batch); err != nil {
				b.Fatal(err)
			}
		}
		benchDisjIdx = idx
	})
	return benchDisjIdx
}

var sinkOptimized index.Optimized

func BenchmarkDisjunctionUnadornedFinish(b *testing.B) {
	idx := getBenchDisjIdx(b)
	r, err := idx.Reader()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	ctx := context.TODO()
	terms := [][]byte{[]byte("t0"), []byte("t1"), []byte("t2"), []byte("t3")}

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		var octx index.OptimizableContext
		tfrs := make([]index.TermFieldReader, 0, len(terms))
		for _, t := range terms {
			tfr, err := r.TermFieldReader(ctx, t, "f", false, false, false)
			if err != nil {
				b.Fatal(err)
			}
			tfrs = append(tfrs, tfr)
			opt := tfr.(index.Optimizable)
			octx, err = opt.Optimize("disjunction:unadorned", octx)
			if err != nil {
				b.Fatal(err)
			}
		}
		o, err := octx.Finish()
		if err != nil {
			b.Fatal(err)
		}
		if o == nil {
			b.Fatal("optimization aborted (nil result)")
		}
		sinkOptimized = o
		for _, tfr := range tfrs {
			_ = tfr.Close()
		}
	}
}
