// Copyright (c) 2026 The Bleve Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fusion

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

type fusionBenchmarkCase struct {
	name       string
	hits       int
	knnQueries int
	window     int
	knnPercent int
}

var fusionBenchmarkCases = []fusionBenchmarkCase{
	{name: "Small", hits: 32, knnQueries: 1, window: 32, knnPercent: 100},
	{name: "Dense", hits: 1000, knnQueries: 3, window: 100, knnPercent: 100},
	{name: "Sparse", hits: 1000, knnQueries: 3, window: 100, knnPercent: 25},
	{name: "ManySources", hits: 1000, knnQueries: 8, window: 100, knnPercent: 12},
	{name: "WideWindow", hits: 1000, knnQueries: 3, window: 1000, knnPercent: 25},
}

type fusionBenchmarkFunc func(search.DocumentMatchCollection, []float64, int, int, bool) *FusionResult

func BenchmarkFusion(b *testing.B) {
	benchmarkFusionAlgorithms(b, false)
}

// BenchmarkFusionPrepareAndFuseParallel includes resetting the preallocated
// input in its timing. Stopping the shared timer from parallel workers would
// exclude other workers' fusion calls as well.
func BenchmarkFusionPrepareAndFuseParallel(b *testing.B) {
	benchmarkFusionAlgorithms(b, true)
}

func benchmarkFusionAlgorithms(b *testing.B, parallel bool) {
	algorithms := []struct {
		name string
		fuse fusionBenchmarkFunc
	}{
		{"RRF", func(hits search.DocumentMatchCollection, weights []float64, window, queries int, explain bool) *FusionResult {
			return ReciprocalRankFusion(hits, weights, 60, window, queries, explain)
		}},
		{"RSF", RelativeScoreFusion},
	}
	for _, algorithm := range algorithms {
		b.Run(algorithm.name, func(b *testing.B) {
			for _, workload := range fusionBenchmarkCases {
				if parallel && workload.name != "Dense" && workload.name != "Sparse" {
					continue
				}
				name := fmt.Sprintf("%s/N%d/Q%d/W%d", workload.name, workload.hits, workload.knnQueries, workload.window)
				b.Run(name, func(b *testing.B) {
					for _, explain := range []bool{false, true} {
						b.Run(fmt.Sprintf("Explain=%t", explain), func(b *testing.B) {
							benchmarkFusionWorkload(b, workload, algorithm.fuse, explain, parallel)
						})
					}
				})
			}
		})
	}
}

func benchmarkFusionWorkload(b *testing.B, workload fusionBenchmarkCase, fuse fusionBenchmarkFunc, explain, parallel bool) {
	template := makeFusionBenchmarkTemplate(workload, explain)
	weights := make([]float64, workload.knnQueries+1)
	weights[0] = 0.4
	for i := 1; i < len(weights); i++ {
		weights[i] = 0.6 / float64(workload.knnQueries)
	}
	b.ReportAllocs()

	if parallel {
		// RunParallel uses GOMAXPROCS workers at its default parallelism.
		// Allocate each worker's mutable state before starting the timer.
		inputs := make(chan *fusionBenchmarkInput, runtime.GOMAXPROCS(0))
		for i := 0; i < cap(inputs); i++ {
			inputs <- newFusionBenchmarkInput(template)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			input := <-inputs
			var result *FusionResult
			for pb.Next() {
				input.reset()
				result = fuse(input.hits, weights, workload.window, workload.knnQueries, explain)
			}
			runtime.KeepAlive(result)
		})
		return
	}

	input := newFusionBenchmarkInput(template)
	var result *FusionResult
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		input.reset()
		b.StartTimer()
		result = fuse(input.hits, weights, workload.window, workload.knnQueries, explain)
	}
	runtime.KeepAlive(result)
}

func makeFusionBenchmarkTemplate(workload fusionBenchmarkCase, explain bool) []search.DocumentMatch {
	rng := rand.New(rand.NewSource(37))
	hits := make([]search.DocumentMatch, workload.hits)
	ftsHits := 0
	for i := range hits {
		hit := &hits[i]
		hit.ID = fmt.Sprintf("doc-%06d", i)
		hit.HitNumber = uint64(i + 1)
		if rng.Intn(4) != 0 && ftsHits < workload.window {
			hit.Score = 1 + float64(rng.Intn(1024))/256
			ftsHits++
		} else {
			// KNN-only hits commonly retain the default hit number.
			hit.HitNumber = 0
		}
		hit.ScoreBreakdown = make(map[int]float64, workload.knnQueries)
		for query := 0; query < workload.knnQueries; query++ {
			if rng.Intn(100) < workload.knnPercent {
				hit.ScoreBreakdown[query] = 0.05 + 0.9*float64(rng.Intn(1024))/1024
			}
		}
		// The input is a union of source results, so every hit needs a source.
		if hit.Score == 0 && len(hit.ScoreBreakdown) == 0 {
			hit.ScoreBreakdown[i%workload.knnQueries] = 0.05 + 0.9*float64(rng.Intn(1024))/1024
		}
		if explain {
			children := make([]*search.Explanation, workload.knnQueries+1)
			children[0] = &search.Explanation{Value: hit.Score, Message: "original FTS score"}
			for query := 0; query < workload.knnQueries; query++ {
				children[query+1] = &search.Explanation{Value: hit.ScoreBreakdown[query], Message: "original KNN score"}
			}
			hit.Expl = &search.Explanation{Value: hit.Score, Message: "original scores", Children: children}
		}
	}
	rng.Shuffle(len(hits), func(i, j int) {
		hits[i], hits[j] = hits[j], hits[i]
	})
	return hits
}

type fusionBenchmarkInput struct {
	template  []search.DocumentMatch
	documents []search.DocumentMatch
	hits      search.DocumentMatchCollection
	roots     []search.Explanation
}

func newFusionBenchmarkInput(template []search.DocumentMatch) *fusionBenchmarkInput {
	input := &fusionBenchmarkInput{
		template:  template,
		documents: make([]search.DocumentMatch, len(template)),
		hits:      make(search.DocumentMatchCollection, len(template)),
	}
	if len(template) > 0 && template[0].Expl != nil {
		input.roots = make([]search.Explanation, len(template))
	}
	return input
}

func (input *fusionBenchmarkInput) reset() {
	for i := range input.template {
		original := &input.template[i]
		hit := &input.documents[i]
		*hit = *original
		if original.Expl != nil {
			// Fusion reads the source map and explanation children without mutating
			// them. Only the DocumentMatch and explanation root require resetting.
			input.roots[i] = *original.Expl
			hit.Expl = &input.roots[i]
		}
		input.hits[i] = hit
	}
}
