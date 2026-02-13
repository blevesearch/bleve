//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build vectors
// +build vectors

package bleve

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

// var datasetPath = "/Users/likithb/Desktop/Code/MB-62985/debug/dataset/cohere.jsonl"
// var parentPath = "/Users/likithb/Desktop/Code/MB-62985/indexes"
// var queryPath = "/Users/likithb/Desktop/Code/MB-62985/debug/queries1k.jsonl"

// func TestBuild500k(t *testing.T) {
// 	f, err := os.Open(datasetPath)
// 	if err != nil {
// 		t.Fatalf("open dataset: %v", err)
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	buf := make([]byte, 0, 1024*1024)
// 	scanner.Buffer(buf, 128*1024*1024)
// 	dims := 768

// 	indexMapping := mapping.NewIndexMapping()

// 	// Recall-optimized vector field mapping
// 	vecFieldFloatRecall := mapping.NewVectorFieldMapping()
// 	vecFieldFloatRecall.Similarity = index.CosineSimilarity
// 	vecFieldFloatRecall.Dims = dims
// 	vecFieldFloatRecall.VectorIndexOptimizedFor = index.IndexOptimizedForRecall

// 	indexMapping.DefaultMapping.AddFieldMappingsAt("recall", vecFieldFloatRecall)

// 	// Latency-optimized vector field mapping
// 	vecFieldFloatLatency := mapping.NewVectorFieldMapping()
// 	vecFieldFloatLatency.Similarity = index.CosineSimilarity
// 	vecFieldFloatLatency.Dims = dims
// 	vecFieldFloatLatency.VectorIndexOptimizedFor = index.IndexOptimizedForLatency

// 	indexMapping.DefaultMapping.AddFieldMappingsAt("latency", vecFieldFloatLatency)

// 	// Memory-efficient vector field mapping
// 	vecFieldFloatMemory := mapping.NewVectorFieldMapping()
// 	vecFieldFloatMemory.Similarity = index.CosineSimilarity
// 	vecFieldFloatMemory.Dims = dims
// 	vecFieldFloatMemory.VectorIndexOptimizedFor = index.IndexOptimizedForMemoryEfficient

// 	indexMapping.DefaultMapping.AddFieldMappingsAt("memory", vecFieldFloatMemory)

// 	// Binary-optimized vector field mapping
// 	vecFieldBinary := mapping.NewVectorFieldMapping()
// 	vecFieldBinary.Similarity = index.CosineSimilarity
// 	vecFieldBinary.Dims = dims
// 	vecFieldBinary.VectorIndexOptimizedFor = index.IndexOptimizedForBinary

// 	indexMapping.DefaultMapping.AddFieldMappingsAt("binary", vecFieldBinary)

// 	indexPath := parentPath + "/test6-50k"
// 	if err := os.RemoveAll(indexPath); err != nil {
// 		t.Fatalf("remove index path: %v", err)
// 	}

// 	idx, err := New(indexPath, indexMapping)
// 	if err != nil {
// 		t.Fatalf("create index: %v", err)
// 	}

// 	defer idx.Close()

// 	batchSize := 500
// 	batch := idx.NewBatch()
// 	numDocs := 50000
// 	count := 0

// 	for scanner.Scan() {
// 		if count%batchSize == 0 && count > 0 {
// 			if err := idx.Batch(batch); err != nil {
// 				t.Fatalf("index batch at doc %d: %v", count, err)
// 			}
// 			batch = idx.NewBatch()
// 			fmt.Printf("indexed %d documents\n", count)
// 		}
// 		if count >= numDocs {
// 			break
// 		}
// 		line := strings.TrimSpace(scanner.Text())
// 		if line == "" {
// 			continue
// 		}
// 		_, vec, ok := parseDoc(line)
// 		if !ok || len(vec) != dims {
// 			t.Fatalf("invalid doc at line %d", count)
// 			continue
// 		}
// 		if err := batch.Index(
// 			fmt.Sprintf("doc-%d", count),
// 			map[string]interface{}{
// 				"recall":  vec,
// 				"latency": vec,
// 				"memory":  vec,
// 				"binary":  vec,
// 			},
// 		); err != nil {
// 			t.Fatalf("index doc %d: %v", count, err)
// 		}
// 		count++
// 	}
// 	if err := scanner.Err(); err != nil {
// 		t.Fatalf("read dataset: %v", err)
// 	}
// 	if err := idx.Batch(batch); err != nil {
// 		t.Fatalf("index final batch: %v", err)
// 	}

// 	time.Sleep(15 * time.Second)
// }

// func TestGenQueries500k(t *testing.T) {

// 	f, err := os.Open(datasetPath)
// 	if err != nil {
// 		t.Fatalf("open dataset: %v", err)
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	buf := make([]byte, 0, 1024*1024)
// 	scanner.Buffer(buf, 128*1024*1024)

// 	if err := os.RemoveAll(queryPath); err != nil {
// 		t.Fatalf("remove index path: %v", err)
// 	}
// 	queriesFile, err := os.Create(queryPath)
// 	if err != nil {
// 		t.Fatalf("create queries file: %v", err)
// 	}
// 	defer queriesFile.Close()
// 	writer := bufio.NewWriter(queriesFile)
// 	numQueries := 2000

// 	// Read 500 vectors and pick one query vector at random and repeat till numQueries or dataset ends
// 	batchSize := 500
// 	var batch []map[string]interface{}
// 	count := 0
// 	queriesGenerated := 0

// 	for scanner.Scan() && queriesGenerated < numQueries {
// 		line := strings.TrimSpace(scanner.Text())
// 		if line == "" {
// 			continue
// 		}

// 		var obj map[string]interface{}
// 		if err := json.Unmarshal([]byte(line), &obj); err != nil {
// 			continue
// 		}

// 		batch = append(batch, obj)

// 		// Process batch when it reaches batchSize
// 		if len(batch) == batchSize {
// 			if len(batch) > 0 {
// 				// Pick a random vector from the batch
// 				randomIdx := rand.Intn(len(batch))
// 				queryObj := batch[randomIdx]

// 				// Write the query to file
// 				queryBytes, err := json.Marshal(queryObj)
// 				if err != nil {
// 					batch = nil
// 					count++
// 					continue
// 				}

// 				if _, err := writer.WriteString(string(queryBytes) + "\n"); err != nil {
// 					t.Fatalf("write query: %v", err)
// 				}

// 				queriesGenerated++
// 				if queriesGenerated%100 == 0 {
// 					fmt.Printf("Generated query %d\tCount %d\n", queriesGenerated, count)
// 				}
// 			}

// 			batch = nil
// 		}

// 		count++
// 	}

// 	// Process remaining batch at end of file
// 	if len(batch) > 0 && queriesGenerated < numQueries {
// 		randomIdx := rand.Intn(len(batch))
// 		queryObj := batch[randomIdx]

// 		queryBytes, err := json.Marshal(queryObj)
// 		if err == nil {
// 			if _, err := writer.WriteString(string(queryBytes) + "\n"); err != nil {
// 				t.Fatalf("write query: %v", err)
// 			}
// 			fmt.Printf("Generated query %d\tCount %d\n", queriesGenerated+1, count)
// 			queriesGenerated++
// 		}
// 	}

// 	if err := scanner.Err(); err != nil {
// 		t.Fatalf("read dataset: %v", err)
// 	}

// 	if err := writer.Flush(); err != nil {
// 		t.Fatalf("flush queries file: %v", err)
// 	}
// }

// func TestRecall500k(t *testing.T) {

// 	queryVecs := readQueryVecs(t)

// 	indexPath := parentPath + "/test6-50k"

// 	idx, err := Open(indexPath)
// 	if err != nil {
// 		t.Fatalf("open index: %v", err)
// 	}
// 	defer idx.Close()

// 	totalRecall := 0.0
// 	for i, qv := range queryVecs {
// 		srRec := NewSearchRequest(NewMatchNoneQuery())
// 		srRec.AddKNN("recall", qv, 100, 1.0)
// 		srRec.Size = 100

// 		hitsRecall, err := idx.Search(srRec)
// 		if err != nil {
// 			t.Fatalf("search recall: %v", err)
// 		}

// 		srBin := NewSearchRequest(NewMatchNoneQuery())
// 		srBin.AddKNN("binary", qv, 100, 1.0)
// 		srBin.Size = 100

// 		hitsBin, err := idx.Search(srBin)
// 		if err != nil {
// 			t.Fatalf("search binary: %v", err)
// 		}

// 		recallSet := make(map[string]struct{})
// 		for _, hit := range hitsRecall.Hits {
// 			recallSet[hit.ID] = struct{}{}
// 		}

// 		matching := 0
// 		for _, hit := range hitsBin.Hits {
// 			if _, found := recallSet[hit.ID]; found {
// 				matching++
// 			}
// 		}

// 		recallRate := float64(matching) / float64(len(hitsRecall.Hits))
// 		fmt.Printf("Query - %d\tRecall Rate - %f\n", i, recallRate)
// 		totalRecall += recallRate
// 	}

// 	avgRecall := totalRecall / float64(len(queryVecs))
// 	fmt.Printf("Average Recall Rate over %d queries: %f\n", len(queryVecs), avgRecall)
// }

// func BenchmarkSearchBinary500k(b *testing.B) {
// 	queryVecs := readQueryVecsB(b)

// 	indexPath := parentPath + "/test1-500k"
// 	idx, err := Open(indexPath)
// 	if err != nil {
// 		b.Fatalf("open index: %v", err)
// 	}
// 	defer idx.Close()

// 	// Pre-build search requests
// 	reqs := make([]*SearchRequest, len(queryVecs))
// 	for i, qv := range queryVecs {
// 		sr := NewSearchRequest(NewMatchNoneQuery())
// 		sr.AddKNN("binary", qv, 100, 1.0)
// 		sr.Size = 100
// 		reqs[i] = sr
// 	}

// 	b.ResetTimer()

// 	for i := 0; i < 1000; i++ {
// 		req := reqs[i]

// 		_, err := idx.Search(req)
// 		if err != nil {
// 			b.Fatalf("search binary: %v", err)
// 		}
// 	}
// }

// func readQueryVecs(t *testing.T) [][]float32 {
// 	f, err := os.Open(queryPath)
// 	if err != nil {
// 		t.Fatalf("open queries file: %v", err)
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	var queryVecs [][]float32

// 	for scanner.Scan() {
// 		line := strings.TrimSpace(scanner.Text())
// 		if line == "" {
// 			continue
// 		}
// 		_, vec, ok := parseDoc(line)
// 		if !ok {
// 			t.Fatalf("invalid query line: %s", line)
// 		}
// 		queryVecs = append(queryVecs, vec)
// 	}
// 	if err := scanner.Err(); err != nil {
// 		t.Fatalf("read queries file: %v", err)
// 	}
// 	return queryVecs
// }

// func readQueryVecsB(t *testing.B) [][]float32 {
// 	f, err := os.Open(queryPath)
// 	if err != nil {
// 		t.Fatalf("open queries file: %v", err)
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	var queryVecs [][]float32

// 	for scanner.Scan() {
// 		line := strings.TrimSpace(scanner.Text())
// 		if line == "" {
// 			continue
// 		}
// 		_, vec, ok := parseDoc(line)
// 		if !ok {
// 			t.Fatalf("invalid query line: %s", line)
// 		}
// 		queryVecs = append(queryVecs, vec)
// 	}
// 	if err := scanner.Err(); err != nil {
// 		t.Fatalf("read queries file: %v", err)
// 	}
// 	return queryVecs
// }

// func parseDoc(line string) (string, []float32, bool) {
// 	var obj map[string]interface{}
// 	if err := json.Unmarshal([]byte(line), &obj); err != nil {
// 		return "", nil, false
// 	}
// 	// txt, _ := obj["text"].(string)
// 	vec := anyToFloat32Slice(obj["emb"])
// 	if len(vec) == 0 {
// 		return "", nil, false
// 	}
// 	return "", vec, true
// }

// Helper function to convert various numeric types to []float32
func anyToFloat32Slice(v interface{}) []float32 {
	switch arr := v.(type) {
	case []interface{}:
		out := make([]float32, 0, len(arr))
		for _, it := range arr {
			switch n := it.(type) {
			case float64:
				out = append(out, float32(n))
			case float32:
				out = append(out, n)
			case int:
				out = append(out, float32(n))
			case int64:
				out = append(out, float32(n))
			default:
				return nil
			}
		}
		return out
	case []float64:
		out := make([]float32, len(arr))
		for i := range arr {
			out[i] = float32(arr[i])
		}
		return out
	case []float32:
		return arr
	default:
		return nil
	}
}

var datasetPath = "/Users/likithb/Desktop/Code/MB-62985/debug/dataset/cohere.jsonl"
var queryPath = "/Users/likithb/Desktop/Code/MB-62985/debug/queries.json"
var groundTruthPath = "/Users/likithb/Desktop/Code/MB-62985/debug/groundTruth.json"
var indexDirectory = "/Users/likithb/Desktop/Code/MB-62985/indexes"

func TestBinaryVectorSegment(t *testing.T) {

	numDocs := 1000
	dims := 768
	ver := 3
	vectors := loadVectors(t, datasetPath, numDocs)
	// vectors := make([][]float32, numDocs)
	// for i := 0; i < numDocs; i++ {
	// 	vectors[i] = make([]float32, dims)
	// 	for j := 0; j < dims; j++ {
	// 		vectors[i][j] = randomFloat32(-1.0, 1.0)
	// 	}
	// }
	// fmt.Printf("loaded %d vectors from %s\n\n", len(vectors), datasetPath)

	indexMapping := buildIndexMapping(dims)

	indexPath := fmt.Sprintf("%s/%d-%dk", indexDirectory, ver, numDocs/1000)
	if err := os.RemoveAll(indexPath); err != nil {
		t.Fatalf("remove index path: %v", err)
	}

	idx, err := New(indexPath, indexMapping)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	defer idx.Close()
	fmt.Printf("created index\n\n")

	batchSize := 1000
	batch := idx.NewBatch()
	count := 0

	for _, vec := range vectors {
		if count%batchSize == 0 && count > 0 {
			if err := idx.Batch(batch); err != nil {
				t.Fatalf("index batch at doc %d: %v", count, err)
			}
			batch = idx.NewBatch()
			fmt.Printf("indexed %d documents\n", count)
		}

		if err := batch.Index(
			fmt.Sprintf("doc-%d", count),
			map[string]interface{}{
				"binary": vec,
				"recall": vec,
			},
		); err != nil {
			t.Fatalf("index doc %d: %v", count, err)
		}
		count++
	}
	if err := idx.Batch(batch); err != nil {
		t.Fatalf("index final batch: %v", err)
	}
	fmt.Printf("indexed total %d documents\n\n", count)
	vectors = nil
	time.Sleep(5 * time.Second)

	// idx, err := Open(indexPath)
	// if err != nil {
	// 	t.Fatalf("open index: %v", err)
	// }
	// defer idx.Close()
	// fmt.Printf("opened index from %s\n\n", indexPath)

	queries := loadQueries(t, queryPath)
	// queries := make([][]float32, 10000)
	// for i := 0; i < 10000; i++ {
	// 	queries[i] = make([]float32, dims)
	// 	for j := 0; j < dims; j++ {
	// 		queries[i][j] = randomFloat32(-1.0, 1.0)
	// 	}
	// }
	fmt.Printf("loaded queries from %s\n\n", queryPath)

	// groundTruths := loadGroundTruths(t, groundTruthPath)
	// fmt.Printf("loaded ground truths from %s\n\n", groundTruthPath)
	totalRecall := 0
	totalTimeBinary := 0
	totalTimeRecall := 0
	k := int64(3)
	for i, qv := range queries {
		start := time.Now()
		sr := NewSearchRequest(NewMatchNoneQuery())
		sr.AddKNN("binary", qv, k, 1.0)
		sr.Size = int(k)

		hits, err := idx.Search(sr)
		if err != nil {
			t.Fatalf("search binary: %v", err)
		}
		elapsed := time.Since(start)
		totalTimeBinary += int(elapsed)
		resultSet := make(map[int64]struct{})
		for _, hit := range hits.Hits {
			var docNum int64
			_, err := fmt.Sscanf(hit.ID, "doc-%d", &docNum)
			if err != nil {
				t.Fatalf("parse doc ID %s: %v", hit.ID, err)
			}
			resultSet[docNum] = struct{}{}
		}

		start = time.Now()
		sr = NewSearchRequest(NewMatchNoneQuery())
		sr.AddKNN("recall", qv, k, 1.0)
		sr.Size = int(k)

		hitsRec, err := idx.Search(sr)
		if err != nil {
			t.Fatalf("search recall: %v", err)
		}
		elapsed = time.Since(start)
		totalTimeRecall += int(elapsed)

		groundTruths := make(map[int64]struct{})
		for _, hit := range hitsRec.Hits {
			var docNum int64
			_, err := fmt.Sscanf(hit.ID, "doc-%d", &docNum)
			if err != nil {
				t.Fatalf("parse doc ID %s: %v", hit.ID, err)
			}
			groundTruths[docNum] = struct{}{}
		}
		matching := 0
		for docNum := range groundTruths {
			if _, found := resultSet[docNum]; found {
				matching++
			}
		}

		// matching := 0
		// for docNum := range groundTruths[i] {
		// 	if _, found := resultSet[docNum]; found {
		// 		matching++
		// 	}
		// }

		totalRecall += matching

		if i%1000 == 0 {
			fmt.Printf("Processed %d queries\n", i+1)
			fmt.Printf("Average Recall so far: %f\n", float64(totalRecall)/float64((i+1)*int(k)))
			fmt.Printf("Average Time per Query (Binary) so far: %v\n", time.Duration(totalTimeBinary/(i+1)))
			fmt.Printf("Average Time per Query (Recall) so far: %v\n", time.Duration(totalTimeRecall/(i+1)))
			if i == 0 {
				totalTimeBinary = 0
				totalTimeRecall = 0
			}
		}
	}

	avgRecall := float64(totalRecall) / float64(len(queries)*int(k))
	fmt.Printf("Average Recall over %d queries: %f\n", len(queries), avgRecall)
	avgTime := time.Duration(totalTimeBinary / (len(queries) - 1))
	fmt.Printf("Average Time per Query (Binary): %v\n", avgTime)
	avgTime = time.Duration(totalTimeRecall / (len(queries) - 1))
	fmt.Printf("Average Time per Query (Recall): %v\n", avgTime)
}

func loadGroundTruths(t *testing.T, path string) []map[int64]struct{} {
	var groundTruth []map[int64]struct{}
	gtFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ground truth file: %v", err)
	}
	defer gtFile.Close()

	decoder := json.NewDecoder(gtFile)
	err = decoder.Decode(&groundTruth)
	if err != nil {
		t.Fatalf("decode ground truth from file: %v", err)
	}
	return groundTruth
}

func randomFloat32(min, max float32) float32 {
	return min + (max-min)*rand.Float32()
}

func loadQueries(t *testing.T, path string) [][]float32 {
	var queries [][]float32
	queryFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("open query file: %v", err)
	}
	defer queryFile.Close()

	decoder := json.NewDecoder(queryFile)
	err = decoder.Decode(&queries)
	if err != nil {
		t.Fatalf("decode queries from file: %v", err)
	}
	return queries
}

func buildIndexMapping(dims int) *mapping.IndexMappingImpl {
	indexMapping := mapping.NewIndexMapping()

	// Recall-optimized vector field mapping
	vecFieldFloatRecall := mapping.NewVectorFieldMapping()
	vecFieldFloatRecall.Similarity = index.CosineSimilarity
	vecFieldFloatRecall.Dims = dims
	vecFieldFloatRecall.VectorIndexOptimizedFor = index.IndexOptimizedForRecall

	indexMapping.DefaultMapping.AddFieldMappingsAt("recall", vecFieldFloatRecall)

	// Latency-optimized vector field mapping
	vecFieldFloatLatency := mapping.NewVectorFieldMapping()
	vecFieldFloatLatency.Similarity = index.CosineSimilarity
	vecFieldFloatLatency.Dims = dims
	vecFieldFloatLatency.VectorIndexOptimizedFor = index.IndexOptimizedForLatency

	indexMapping.DefaultMapping.AddFieldMappingsAt("latency", vecFieldFloatLatency)

	// Memory-efficient vector field mapping
	vecFieldFloatMemory := mapping.NewVectorFieldMapping()
	vecFieldFloatMemory.Similarity = index.CosineSimilarity
	vecFieldFloatMemory.Dims = dims
	vecFieldFloatMemory.VectorIndexOptimizedFor = index.IndexOptimizedForMemoryEfficient

	indexMapping.DefaultMapping.AddFieldMappingsAt("memory", vecFieldFloatMemory)

	// Binary-optimized vector field mapping
	vecFieldBinary := mapping.NewVectorFieldMapping()
	vecFieldBinary.Similarity = index.CosineSimilarity
	vecFieldBinary.Dims = dims
	vecFieldBinary.VectorIndexOptimizedFor = index.IndexOptimizedWithBivfFlat

	indexMapping.DefaultMapping.AddFieldMappingsAt("binary", vecFieldBinary)

	return indexMapping
}

func loadVectors(t *testing.T, path string, maxDocs int) [][]float32 {
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open dataset: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 128*1024*1024)

	var vectors [][]float32
	count := 0

	for scanner.Scan() && count < maxDocs {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}

		vec := anyToFloat32Slice(obj["emb"])
		vectors = append(vectors, vec)
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read dataset: %v", err)
	}

	return vectors
}

func TestCreateDistribution(t *testing.T) {
	numVectors := 100000
	vectors := loadVectors(t, datasetPath, numVectors)
	buckets := make([]int, 40)

	for i := 0; i < numVectors; i++ {
		for j := 0; j < 768; j++ {
			val := vectors[i][j]
			bucketIdx := int((val + 2.0) / 0.1)
			if bucketIdx < 0 {
				bucketIdx = 0
			} else if bucketIdx >= len(buckets) {
				bucketIdx = len(buckets) - 1
			}
			buckets[bucketIdx]++
		}
	}

	for i, count := range buckets {
		lowerBound := -2.0 + float32(i)*0.1
		upperBound := lowerBound + 0.1
		fmt.Printf("Bucket %2d [%.1f, %.1f): %d\n", i, lowerBound, upperBound, count)
	}
	total := 0
	for _, count := range buckets {
		total += count
	}
	fmt.Printf("Total values: %d\n", total)
	fmt.Printf("Expected: %d\n", numVectors*768)
}
